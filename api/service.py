import os
from datetime import datetime, timezone
from functools import lru_cache
from typing import Dict, Optional, Union

from cassandra.cluster import Cluster

from fraud_pipeline import (
    PipelineConfig,
    RECEIVER_STATE_TOPIC,
    SENDER_STATE_TOPIC,
    TRANSACTION_TOPIC,
    RuleEngine,
    TransactionEvent,
    derive_account_state_updates,
    prediction_record_from_decision,
    receiver_state_to_dict,
    sender_state_to_dict,
    transaction_to_dict,
)
from fraud_pipeline.kafka_client import create_kafka_producer_with_retry
from fraud_pipeline.parsing import build_event_id
from fraud_pipeline.serialization import dumps

from .schemas import ScoreRequest, ScoreResponse


os.environ.setdefault("FRAUD_MODEL_TYPE", "v1")

try:
    from model.model_utils import get_model_version, model_is_loaded
except ImportError:
    def model_is_loaded() -> bool:
        return False

    def get_model_version() -> str:
        return "v0"


@lru_cache(maxsize=1)
def get_rule_engine() -> RuleEngine:
    return RuleEngine(PipelineConfig())


def get_model_type() -> str:
    return os.getenv("FRAUD_MODEL_TYPE", "xgb")


def kafka_ingest_enabled() -> bool:
    return bool(os.getenv("KAFKA_BOOTSTRAP_SERVERS"))


def get_kafka_bootstrap_servers() -> str:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")


def prediction_logging_enabled() -> bool:
    return os.getenv("API_PREDICTION_LOGGING_ENABLED", "0") == "1"


class KafkaPublishError(RuntimeError):
    pass


@lru_cache(maxsize=1)
def get_prediction_log_session() -> Optional[object]:
    if not prediction_logging_enabled():
        return None

    host = os.getenv("CASSANDRA_HOST", "localhost")
    port = int(os.getenv("CASSANDRA_PORT", "9042"))
    keyspace = os.getenv("CASSANDRA_KEYSPACE", "fraud_detection")
    try:
        cluster = Cluster([host], port=port)
        return cluster.connect(keyspace)
    except Exception:
        return None


@lru_cache(maxsize=1)
def get_prediction_insert_statement():
    session = get_prediction_log_session()
    if session is None:
        return None
    return session.prepare(
        """
        INSERT INTO model_predictions_by_day (
          day_bucket, event_ts, event_id, account_id, name_dest, txn_type, amount,
          risk_score, severity, ml_score, ml_model_version, triggered_rules,
          is_alert, alert_id, actual_label
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )


@lru_cache(maxsize=1)
def get_ingest_producer():
    bootstrap_servers = get_kafka_bootstrap_servers()
    if not bootstrap_servers:
        return None
    return create_kafka_producer_with_retry(
        bootstrap_servers,
        value_serializer=lambda value: dumps(value),
        key_serializer=lambda value: value.encode("utf-8"),
    )


def persist_prediction_record(prediction) -> None:
    session = get_prediction_log_session()
    statement = get_prediction_insert_statement()
    if session is None or statement is None:
        return

    try:
        event_ts = prediction.event_time
        if event_ts.tzinfo is not None:
            event_ts = event_ts.astimezone(timezone.utc).replace(tzinfo=None)
        session.execute(
            statement,
            (
                prediction.event_time.date(),
                event_ts,
                prediction.event_id,
                prediction.account_id,
                prediction.name_dest,
                prediction.txn_type,
                prediction.amount,
                prediction.risk_score,
                prediction.severity,
                prediction.ml_score,
                prediction.ml_model_version,
                list(prediction.triggered_rules),
                prediction.is_alert,
                prediction.alert_id,
                None,
            ),
        )
    except Exception:
        return


def publish_transaction_bundle(event: TransactionEvent) -> None:
    if not kafka_ingest_enabled():
        return

    producer = get_ingest_producer()
    if producer is None:
        raise KafkaPublishError("Kafka ingest is enabled but producer could not be created.")

    sender_update, receiver_update = derive_account_state_updates(event)
    futures = [
        producer.send(
            TRANSACTION_TOPIC,
            key=event.event_id,
            value=transaction_to_dict(event),
        ),
        producer.send(
            SENDER_STATE_TOPIC,
            key=sender_update.source_event_id,
            value=sender_state_to_dict(sender_update),
        ),
        producer.send(
            RECEIVER_STATE_TOPIC,
            key=receiver_update.source_event_id,
            value=receiver_state_to_dict(receiver_update),
        ),
    ]
    try:
        for future in futures:
            future.get(timeout=10)
        producer.flush()
    except Exception as exc:
        raise KafkaPublishError(f"Failed to publish transaction bundle to Kafka: {exc}") from exc


def build_transaction_event_from_request(payload: ScoreRequest) -> TransactionEvent:
    event_time = payload.event_time or datetime.now(timezone.utc)
    producer_ts = payload.producer_ts or event_time

    row = {
        "step": str(payload.step),
        "type": payload.txn_type,
        "amount": str(payload.amount),
        "nameOrig": payload.name_orig,
        "nameDest": payload.name_dest,
        "oldbalanceOrg": str(payload.oldbalance_org),
        "newbalanceOrig": str(payload.newbalance_orig),
        "oldbalanceDest": str(payload.oldbalance_dest),
        "newbalanceDest": str(payload.newbalance_dest),
        "isFraud": str(payload.is_fraud),
    }
    event_id = payload.event_id or build_event_id(row)

    return TransactionEvent(
        event_id=event_id,
        event_time=event_time,
        producer_ts=producer_ts,
        step=payload.step,
        txn_type=payload.txn_type,
        amount=payload.amount,
        name_orig=payload.name_orig,
        oldbalance_org=payload.oldbalance_org,
        newbalance_orig=payload.newbalance_orig,
        name_dest=payload.name_dest,
        oldbalance_dest=payload.oldbalance_dest,
        newbalance_dest=payload.newbalance_dest,
        is_fraud=payload.is_fraud,
        schema_version=payload.schema_version,
    )


def score_transaction(payload: ScoreRequest) -> ScoreResponse:
    event = build_transaction_event_from_request(payload)
    decision = get_rule_engine().evaluate(event)
    prediction = prediction_record_from_decision(event, decision)
    persist_prediction_record(prediction)
    publish_transaction_bundle(event)
    return ScoreResponse(
        event_id=prediction.event_id,
        is_alert=prediction.is_alert,
        risk_score=prediction.risk_score,
        severity=prediction.severity,
        ml_score=prediction.ml_score,
        ml_model_version=prediction.ml_model_version,
        triggered_rules=list(prediction.triggered_rules),
    )


def health_payload() -> Dict[str, Union[str, bool]]:
    return {
        "status": "ok",
        "model_loaded": model_is_loaded(),
        "model_version": get_model_version(),
        "model_type": get_model_type(),
        "prediction_logging_enabled": prediction_logging_enabled(),
    }
