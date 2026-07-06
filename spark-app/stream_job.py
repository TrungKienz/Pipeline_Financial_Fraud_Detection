#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import time
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any

import redis
from cassandra.cluster import Cluster
from kafka import KafkaProducer
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from fraud_pipeline import (
    FRAUD_ALERTS_TOPIC,
    METRICS_WINDOWED_TOPIC,
    PIPELINE_DEAD_LETTER_TOPIC,
    PipelineConfig,
    PredictionRecord,
    RuleEngine,
    WindowMetric,
    fraud_decision_to_dict,
    integrated_payload_to_transaction_event,
    prediction_record_from_decision,
    window_metric_to_dict,
)
from fraud_pipeline.cassandra_schema import ensure_schema
from fraud_pipeline.kafka_rules import RuntimeRuleState, build_runtime_rule_state, load_runtime_rule_state
from fraud_pipeline.models import TransactionEvent
from fraud_pipeline.runtime_state import read_or_create_pipeline_run_id, scoped_query_name
from fraud_pipeline.topics import RECEIVER_STATE_TOPIC, SENDER_STATE_TOPIC, TRANSACTION_TOPIC


APP_NAME = "RealtimeFraud3StreamIntegration"
WATERMARK_DELAY = "10 minutes"
JOIN_TOLERANCE = "30 seconds"
TUMBLING_WINDOW = "5 minutes"
SLIDING_WINDOW = "10 minutes"
SLIDE_INTERVAL = "5 minutes"
BENCHMARK_EVENT_PREFIX = "bench:"
RULE_REFRESH_SECONDS = int(os.getenv("RISK_RULE_REFRESH_SECONDS", "0"))
CHECKPOINT_ROOT = os.path.join(os.getenv("SPARK_CHECKPOINT_ROOT", "/tmp/spark_checkpoints"), "v4_clean")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
STARTING_OFFSETS = os.getenv("PIPELINE_STARTING_OFFSETS", "earliest")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "fraud_detection")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
BASE_CONFIG = PipelineConfig()

_RESOURCE_CACHE: dict[str, Any] = {}
_RULE_STATE_CACHE: dict[str, Any] = {"loaded_at": 0.0, "state": None}


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName(APP_NAME)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.statefulOperator.allowMultiple", "true")
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
        .config("spark.sql.debug.maxToStringFields", "1000")
        .config("spark.sql.streaming.checkpointLocation", f"{CHECKPOINT_ROOT}/metadata")
        .config("spark.ui.prometheus.enabled", "true")
        .getOrCreate()
    )


def transaction_schema() -> StructType:
    return StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("event_time", StringType(), False),
            StructField("producer_ts", StringType(), False),
            StructField("step", IntegerType(), False),
            StructField("type", StringType(), False),
            StructField("amount", DoubleType(), False),
            StructField("nameOrig", StringType(), False),
            StructField("nameDest", StringType(), False),
            StructField("isFraud", IntegerType(), False),
            StructField("schema_version", IntegerType(), False),
        ]
    )


def sender_state_schema() -> StructType:
    return StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("source_event_id", StringType(), False),
            StructField("event_time", StringType(), False),
            StructField("step", IntegerType(), False),
            StructField("nameOrig", StringType(), False),
            StructField("oldbalanceOrg", DoubleType(), False),
            StructField("newbalanceOrig", DoubleType(), False),
        ]
    )


def receiver_state_schema() -> StructType:
    return StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("source_event_id", StringType(), False),
            StructField("event_time", StringType(), False),
            StructField("step", IntegerType(), False),
            StructField("nameDest", StringType(), False),
            StructField("oldbalanceDest", DoubleType(), False),
            StructField("newbalanceDest", DoubleType(), False),
        ]
    )


def create_kafka_stream(spark: SparkSession, topic: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr(
            "CAST(key AS STRING) AS kafka_key",
            "CAST(value AS STRING) AS raw_value",
            "timestamp AS kafka_timestamp",
            "partition",
            "offset",
        )
        .withColumn("source_topic", F.lit(topic))
    )


def _required_fields_condition(fields: list[str], prefix: str = "payload") -> F.Column:
    condition = F.lit(False)
    for name in fields:
        condition = condition | F.col(f"{prefix}.{name}").isNull()
    return condition


def build_dead_letter_records(frame: DataFrame) -> DataFrame:
    return frame.select(
        F.concat_ws(
            ":",
            F.col("source_topic"),
            F.coalesce(F.col("source_key"), F.lit("null")),
            F.coalesce(F.col("partition").cast("string"), F.lit("na")),
            F.coalesce(F.col("offset").cast("string"), F.lit("na")),
        ).alias("key"),
        F.to_json(
            F.struct(
                "source_topic",
                "source_key",
                "partition",
                "offset",
                "error",
                "raw_value",
                "observed_at",
            )
        ).alias("value"),
    )


def decode_json_stream(
    raw_df: DataFrame,
    schema: StructType,
    required_fields: list[str],
    timestamp_fields: list[str],
) -> tuple[DataFrame, DataFrame]:
    parsed = raw_df.withColumn("payload", F.from_json("raw_value", schema))
    parse_invalid = parsed.filter(F.col("payload").isNull() | _required_fields_condition(required_fields)).select(
        "source_topic",
        F.col("kafka_key").alias("source_key"),
        "partition",
        "offset",
        "raw_value",
        F.when(F.col("payload").isNull(), F.lit("json_parse_error")).otherwise(
            F.lit("missing_required_fields")
        ).alias("error"),
        F.coalesce(F.col("kafka_timestamp"), F.current_timestamp()).alias("observed_at"),
    )

    projected = parsed.filter(~(F.col("payload").isNull() | _required_fields_condition(required_fields))).select(
        "source_topic",
        F.col("kafka_key").alias("source_key"),
        "partition",
        "offset",
        "raw_value",
        *[F.col(f"payload.{field.name}").alias(field.name) for field in schema.fields],
    )
    for timestamp_field in timestamp_fields:
        projected = projected.withColumn(timestamp_field, F.to_timestamp(F.col(timestamp_field)))

    timestamp_invalid_condition = F.lit(False)
    for timestamp_field in timestamp_fields:
        timestamp_invalid_condition = timestamp_invalid_condition | F.col(timestamp_field).isNull()

    timestamp_invalid = projected.filter(timestamp_invalid_condition).select(
        "source_topic",
        "source_key",
        "partition",
        "offset",
        "raw_value",
        F.lit("invalid_timestamp").alias("error"),
        F.current_timestamp().alias("observed_at"),
    )
    # Data Quality Check: Filter out obvious garbage before processing
    quality_condition = (
        ~(F.col("event_id").isNull() | (F.col("event_id") == ""))
        & (F.col("step") >= 0)
    )
    
    # Check for negative amounts if applicable (e.g., in transactions)
    if "amount" in [f.name for f in schema.fields]:
        quality_condition = quality_condition & (F.col("amount") >= 0)

    valid_format = projected.filter(quality_condition)
    quality_invalid = projected.filter(~quality_condition).select(
        "source_topic",
        "source_key",
        "partition",
        "offset",
        "raw_value",
        F.lit("data_quality_violation").alias("error"),
        F.current_timestamp().alias("observed_at"),
    )

    valid = valid_format.drop("raw_value", "source_key", "partition", "offset")
    # Apply watermark once here, on the primary timestamp column
    if "event_time" in valid.columns:
        valid = valid.withWatermark("event_time", WATERMARK_DELAY)
        
    invalid = build_dead_letter_records(parse_invalid.unionByName(timestamp_invalid).unionByName(quality_invalid))
    return valid, invalid


def build_integrated_stream(
    transactions: DataFrame,
    sender_updates: DataFrame,
    receiver_updates: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    tx = (
        transactions.select(
            F.col("event_id").alias("tx_event_id"),
            F.col("event_time").alias("tx_event_time"),
            F.col("producer_ts").alias("tx_producer_ts"),
            F.col("step").alias("tx_step"),
            F.col("type").alias("tx_type"),
            F.col("amount").alias("tx_amount"),
            F.col("nameOrig").alias("tx_name_orig"),
            F.col("nameDest").alias("tx_name_dest"),
            F.col("isFraud").alias("tx_is_fraud"),
            F.col("schema_version").alias("tx_schema_version"),
        )
    )
    sender = (
        sender_updates.select(
            F.col("event_id").alias("sender_event_id"),
            F.col("source_event_id").alias("sender_source_event_id"),
            F.col("event_time").alias("sender_event_time"),
            F.col("step").alias("sender_step"),
            F.col("nameOrig").alias("sender_name_orig"),
            F.col("oldbalanceOrg").alias("oldbalanceOrg"),
            F.col("newbalanceOrig").alias("newbalanceOrig"),
        )
    )
    receiver = (
        receiver_updates.select(
            F.col("event_id").alias("receiver_event_id"),
            F.col("source_event_id").alias("receiver_source_event_id"),
            F.col("event_time").alias("receiver_event_time"),
            F.col("step").alias("receiver_step"),
            F.col("nameDest").alias("receiver_name_dest"),
            F.col("oldbalanceDest").alias("oldbalanceDest"),
            F.col("newbalanceDest").alias("newbalanceDest"),
        )
    )

    join_tolerance = F.expr(f"INTERVAL {JOIN_TOLERANCE.upper()}")
    def build_dead_letter(
        frame: DataFrame,
        source_topic: str,
        source_key_col: str,
        error: str,
        raw_value: F.Column,
    ) -> DataFrame:
        return frame.select(
            F.concat_ws(
                ":",
                F.lit(source_topic),
                F.coalesce(F.col(source_key_col).cast("string"), F.lit("null")),
                F.lit(error),
            ).alias("key"),
            F.to_json(
                F.struct(
                    F.lit(source_topic).alias("source_topic"),
                    F.col(source_key_col).cast("string").alias("source_key"),
                    F.lit(None).cast("int").alias("partition"),
                    F.lit(None).cast("long").alias("offset"),
                    F.lit(error).alias("error"),
                    raw_value.alias("raw_value"),
                    F.current_timestamp().alias("observed_at"),
                )
            ).alias("value"),
        )

    tx_sender = tx.join(
        sender,
        (F.col("tx_event_id") == F.col("sender_source_event_id"))
        & (F.col("sender_event_time") >= F.col("tx_event_time") - join_tolerance)
        & (F.col("sender_event_time") <= F.col("tx_event_time") + join_tolerance),
        "leftOuter",
    )
    missing_sender_dead_letters = build_dead_letter(
        tx_sender.filter(F.col("sender_event_id").isNull()),
        TRANSACTION_TOPIC,
        "tx_event_id",
        "missing_sender_state",
        F.to_json(
            F.struct(
                F.col("tx_event_id").alias("event_id"),
                F.col("tx_event_time").alias("event_time"),
                F.col("tx_step").alias("step"),
                F.col("tx_name_orig").alias("nameOrig"),
                F.col("tx_name_dest").alias("nameDest"),
            )
        ),
    )

    tx_with_sender = tx_sender.filter(F.col("sender_event_id").isNotNull())
    tx_sender_receiver = tx_with_sender.join(
        receiver,
        (F.col("tx_event_id") == F.col("receiver_source_event_id"))
        & (F.col("receiver_event_time") >= F.col("tx_event_time") - join_tolerance)
        & (F.col("receiver_event_time") <= F.col("tx_event_time") + join_tolerance),
        "leftOuter",
    )
    missing_receiver_dead_letters = build_dead_letter(
        tx_sender_receiver.filter(F.col("receiver_event_id").isNull()),
        TRANSACTION_TOPIC,
        "tx_event_id",
        "missing_receiver_state",
        F.to_json(
            F.struct(
                F.col("tx_event_id").alias("event_id"),
                F.col("tx_event_time").alias("event_time"),
                F.col("tx_step").alias("step"),
                F.col("tx_name_orig").alias("nameOrig"),
                F.col("tx_name_dest").alias("nameDest"),
                F.col("sender_event_id").alias("sender_event_id"),
            )
        ),
    )

    candidates = tx_sender_receiver.filter(F.col("receiver_event_id").isNotNull()).withColumn(
        "sender_semantic_match",
        (F.col("tx_step") == F.col("sender_step")) & (F.col("tx_name_orig") == F.col("sender_name_orig")),
    ).withColumn(
        "receiver_semantic_match",
        (F.col("tx_step") == F.col("receiver_step")) & (F.col("tx_name_dest") == F.col("receiver_name_dest")),
    )

    valid_integrated = (
        candidates.filter(F.col("sender_semantic_match") & F.col("receiver_semantic_match"))
        .select(
            F.col("tx_event_id").alias("event_id"),
            F.col("tx_event_time").alias("event_time"),
            F.col("tx_producer_ts").alias("producer_ts"),
            F.col("tx_step").alias("step"),
            F.col("tx_type").alias("type"),
            F.col("tx_amount").alias("amount"),
            F.col("tx_name_orig").alias("nameOrig"),
            F.col("tx_name_dest").alias("nameDest"),
            "oldbalanceOrg",
            "newbalanceOrig",
            "oldbalanceDest",
            "newbalanceDest",
            F.col("tx_is_fraud").alias("isFraud"),
            F.col("tx_schema_version").alias("schema_version"),
        )
    )

    mismatch_dead_letters = candidates.filter(
        ~(F.col("sender_semantic_match") & F.col("receiver_semantic_match"))
    ).select(
        F.concat_ws(":", F.lit("integrated_join"), F.col("tx_event_id")).alias("key"),
        F.to_json(
            F.struct(
                F.lit("integrated_join").alias("source_topic"),
                F.col("tx_event_id").alias("source_key"),
                F.lit(None).cast("int").alias("partition"),
                F.lit(None).cast("long").alias("offset"),
                F.lit("semantic_key_mismatch").alias("error"),
                F.to_json(
                    F.struct(
                        "tx_event_id",
                        "tx_step",
                        "sender_step",
                        "receiver_step",
                        "tx_name_orig",
                        "sender_name_orig",
                        "tx_name_dest",
                        "receiver_name_dest",
                    )
                ).alias("raw_value"),
                F.current_timestamp().alias("observed_at"),
            )
        ).alias("value"),
    )

    sender_orphan_dead_letters = build_dead_letter(
        sender.join(
            tx,
            (F.col("tx_event_id") == F.col("sender_source_event_id"))
            & (F.col("sender_event_time") >= F.col("tx_event_time") - join_tolerance)
            & (F.col("sender_event_time") <= F.col("tx_event_time") + join_tolerance),
            "leftOuter",
        ).filter(F.col("tx_event_id").isNull()),
        SENDER_STATE_TOPIC,
        "sender_event_id",
        "orphan_sender_state",
        F.to_json(
            F.struct(
                F.col("sender_event_id").alias("event_id"),
                F.col("sender_source_event_id").alias("source_event_id"),
                F.col("sender_event_time").alias("event_time"),
                F.col("sender_step").alias("step"),
                F.col("sender_name_orig").alias("nameOrig"),
            )
        ),
    )
    receiver_orphan_dead_letters = build_dead_letter(
        receiver.join(
            tx,
            (F.col("tx_event_id") == F.col("receiver_source_event_id"))
            & (F.col("receiver_event_time") >= F.col("tx_event_time") - join_tolerance)
            & (F.col("receiver_event_time") <= F.col("tx_event_time") + join_tolerance),
            "leftOuter",
        ).filter(F.col("tx_event_id").isNull()),
        RECEIVER_STATE_TOPIC,
        "receiver_event_id",
        "orphan_receiver_state",
        F.to_json(
            F.struct(
                F.col("receiver_event_id").alias("event_id"),
                F.col("receiver_source_event_id").alias("source_event_id"),
                F.col("receiver_event_time").alias("event_time"),
                F.col("receiver_step").alias("step"),
                F.col("receiver_name_dest").alias("nameDest"),
            )
        ),
    )
    dead_letters = (
        mismatch_dead_letters.unionByName(missing_sender_dead_letters)
        .unionByName(missing_receiver_dead_letters)
        .unionByName(sender_orphan_dead_letters)
        .unionByName(receiver_orphan_dead_letters)
    )
    return valid_integrated, dead_letters


def build_window_metrics(
    integrated_df: DataFrame,
    window_type: str,
    window_duration: str,
    slide_duration: str | None = None,
) -> DataFrame:
    if slide_duration:
        window_column = F.window(F.col("event_time"), window_duration, slide_duration)
    else:
        window_column = F.window(F.col("event_time"), window_duration)
    aggregated = (
        integrated_df.groupBy(window_column)
        .agg(
            F.count("*").alias("event_count"),
            F.sum(F.col("isFraud").cast("long")).alias("fraud_count"),
            F.sum(F.col("amount")).alias("total_amount"),
        )
        .select(
            F.lit(window_type).alias("window_type"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "event_count",
            "fraud_count",
            "total_amount",
        )
        .withColumn(
            "fraud_rate",
            F.when(F.col("event_count") > 0, F.col("fraud_count") / F.col("event_count")).otherwise(F.lit(0.0)),
        )
    )
    return aggregated


def get_cassandra_session():
    if "cassandra_session" not in _RESOURCE_CACHE:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect(CASSANDRA_KEYSPACE)
        _RESOURCE_CACHE["cassandra_cluster"] = cluster
        _RESOURCE_CACHE["cassandra_session"] = session
    return _RESOURCE_CACHE["cassandra_session"]


def get_prepared_statements() -> dict[str, Any]:
    if "prepared_statements" in _RESOURCE_CACHE:
        return _RESOURCE_CACHE["prepared_statements"]
    session = get_cassandra_session()
    statements = {
        "select_batch": session.prepare(
            """
            SELECT batch_id FROM processed_stream_batches
            WHERE query_name = ? AND batch_id = ?
            """
        ),
        "insert_batch": session.prepare(
            """
            INSERT INTO processed_stream_batches (query_name, batch_id, processed_at)
            VALUES (?, ?, ?)
            """
        ),
        "insert_transaction": session.prepare(
            """
            INSERT INTO transactions_by_day (
              day_bucket, event_ts, event_id, account_id, name_dest, txn_type, amount,
              sender_balance_before, sender_balance_after, receiver_balance_before,
              receiver_balance_after, is_fraud, risk_score
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        ),
        "insert_prediction": session.prepare(
            """
            INSERT INTO model_predictions_by_day (
              day_bucket, event_ts, event_id, account_id, name_dest, txn_type, amount,
              risk_score, severity, ml_score, ml_model_version, triggered_rules,
              is_alert, alert_id, actual_label
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        ),
        "insert_alert": session.prepare(
            """
            INSERT INTO alerts_by_account (
              account_id, alert_date, alert_ts, alert_id, event_id, name_dest, txn_type,
              amount, risk_score, severity, ml_score, ml_model_version, triggered_rules
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        ),
        "insert_account_state": session.prepare(
            """
            INSERT INTO account_state_by_account (
              account_id, event_ts, event_id, source_event_id, role, step, balance_before, balance_after
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
        ),
        "insert_metric": session.prepare(
            """
            INSERT INTO metrics_by_window (
              window_type, window_bucket, window_start, window_end, event_count, fraud_count, total_amount, fraud_rate
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
        ),
        "insert_benchmark_stage_timing": session.prepare(
            """
            INSERT INTO benchmark_stage_timings_by_run (
              benchmark_run_id, event_id, spark_seen_at, cassandra_persisted_at
            ) VALUES (?, ?, ?, ?)
            """
        ),
    }
    _RESOURCE_CACHE["prepared_statements"] = statements
    return statements


def get_kafka_producer() -> KafkaProducer:
    if "kafka_producer" not in _RESOURCE_CACHE:
        _RESOURCE_CACHE["kafka_producer"] = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            acks="all",
            enable_idempotence=True,
            linger_ms=5,
            max_in_flight_requests_per_connection=1,
            value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda value: value.encode("utf-8"),
        )
    return _RESOURCE_CACHE["kafka_producer"]


def get_redis_client() -> redis.Redis:
    if "redis_client" not in _RESOURCE_CACHE:
        _RESOURCE_CACHE["redis_client"] = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
        )
    return _RESOURCE_CACHE["redis_client"]


def get_pipeline_run_id() -> str:
    if "pipeline_run_id" in _RESOURCE_CACHE:
        return _RESOURCE_CACHE["pipeline_run_id"]
    run_id = read_or_create_pipeline_run_id(CHECKPOINT_ROOT)
    _RESOURCE_CACHE["pipeline_run_id"] = run_id
    return run_id


def scoped_query_name_for_run(base_name: str) -> str:
    return scoped_query_name(base_name, get_pipeline_run_id())


def batch_already_processed(query_name: str, batch_id: int) -> bool:
    row = get_cassandra_session().execute(get_prepared_statements()["select_batch"], (query_name, batch_id)).one()
    return row is not None


def mark_batch_processed(query_name: str, batch_id: int) -> None:
    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    get_cassandra_session().execute(get_prepared_statements()["insert_batch"], (query_name, batch_id, now_utc))


def to_utc_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def get_runtime_rule_state_cached() -> RuntimeRuleState:
    now = time.time()
    cached = _RULE_STATE_CACHE.get("state")
    if cached is not None:
        if RULE_REFRESH_SECONDS <= 0:
            return cached
        if now - float(_RULE_STATE_CACHE.get("loaded_at", 0.0)) < RULE_REFRESH_SECONDS:
            return cached
    try:
        state = load_runtime_rule_state(KAFKA_BOOTSTRAP_SERVERS, config=BASE_CONFIG)
    except Exception:
        state = build_runtime_rule_state([], config=BASE_CONFIG)
    _RULE_STATE_CACHE["loaded_at"] = now
    _RULE_STATE_CACHE["state"] = state
    return state


def warm_rule_state_cache() -> RuntimeRuleState:
    cached = _RULE_STATE_CACHE.get("state")
    if cached is not None:
        return cached
    return get_runtime_rule_state_cached()


def config_from_rule_state(state: RuntimeRuleState) -> PipelineConfig:
    return replace(
        BASE_CONFIG,
        high_amount_transfer_threshold=state.amount_thresholds.get(
            "TRANSFER", BASE_CONFIG.high_amount_transfer_threshold
        ),
        high_amount_cash_out_threshold=state.amount_thresholds.get("CASH_OUT", BASE_CONFIG.high_amount_cash_out_threshold),
        rapid_outflow_amount_threshold=state.rapid_outflow_amount_threshold,
        rapid_outflow_count_threshold=state.rapid_outflow_count_threshold,
        account_drain_min_balance_floor=state.account_drain_min_balance_floor,
        account_drain_ratio_threshold=state.account_drain_ratio_threshold,
        account_drain_near_zero_balance=state.account_drain_near_zero_balance,
        account_drain_weight=state.account_drain_weight,
        fan_out_window_seconds=state.fan_out_window_seconds,
        fan_out_distinct_receiver_threshold=state.fan_out_distinct_receiver_threshold,
        fan_out_total_amount_threshold=state.fan_out_total_amount_threshold,
        sender_fan_out_weight=state.sender_fan_out_weight,
        fan_in_window_seconds=state.fan_in_window_seconds,
        fan_in_distinct_sender_threshold=state.fan_in_distinct_sender_threshold,
        fan_in_total_amount_threshold=state.fan_in_total_amount_threshold,
        receiver_fan_in_weight=state.receiver_fan_in_weight,
        structuring_window_seconds=state.structuring_window_seconds,
        structuring_count_threshold=state.structuring_count_threshold,
        structuring_min_amount=state.structuring_min_amount,
        structuring_max_amount=state.structuring_max_amount,
        structuring_total_amount_threshold=state.structuring_total_amount_threshold,
        structured_split_weight=state.structured_split_weight,
        new_counterparty_amount_threshold=state.new_counterparty_amount_threshold,
        new_counterparty_weight=state.new_counterparty_weight,
        cashout_after_inbound_window_seconds=state.cashout_after_inbound_window_seconds,
        cashout_after_inbound_ratio_threshold=state.cashout_after_inbound_ratio_threshold,
        cashout_after_inbound_weight=state.cashout_after_inbound_weight,
    )


def build_history_stub(event: TransactionEvent, event_time: datetime, amount: float) -> TransactionEvent:
    return TransactionEvent(
        event_id=f"history:{event.name_orig}:{int(event_time.timestamp())}:{amount}",
        event_time=event_time,
        producer_ts=event_time,
        step=event.step,
        txn_type=event.txn_type,
        amount=amount,
        name_orig=event.name_orig,
        oldbalance_org=event.oldbalance_org,
        newbalance_orig=event.newbalance_orig,
        name_dest=event.name_dest,
        oldbalance_dest=event.oldbalance_dest,
        newbalance_dest=event.newbalance_dest,
        is_fraud=0,
        schema_version=event.schema_version,
    )


def build_transaction_stub(
    template: TransactionEvent,
    event_id: str,
    event_time: datetime,
    txn_type: str,
    amount: float,
    name_orig: str,
    name_dest: str,
) -> TransactionEvent:
    return TransactionEvent(
        event_id=event_id,
        event_time=event_time,
        producer_ts=event_time,
        step=template.step,
        txn_type=txn_type,
        amount=amount,
        name_orig=name_orig,
        oldbalance_org=template.oldbalance_org,
        newbalance_orig=template.newbalance_orig,
        name_dest=name_dest,
        oldbalance_dest=template.oldbalance_dest,
        newbalance_dest=template.newbalance_dest,
        is_fraud=0,
        schema_version=template.schema_version,
    )


def load_recent_sender_events(
    redis_client: redis.Redis,
    event: TransactionEvent,
    config: PipelineConfig,
) -> list[TransactionEvent]:
    history_key = f"sender_history:{event.name_orig}"
    current_ts = int(event.event_time.timestamp())
    lower_bound = current_ts - config.rapid_outflow_window_seconds
    try:
        redis_client.zremrangebyscore(history_key, 0, lower_bound - 1)
        members = redis_client.zrangebyscore(history_key, lower_bound, current_ts)
    except redis.RedisError:
        return []

    history: list[TransactionEvent] = []
    for raw_member in members:
        try:
            payload = json.loads(raw_member)
            event_time = datetime.fromisoformat(payload["event_time"])
            history.append(
                build_transaction_stub(
                    event,
                    payload.get("event_id", f"history:{event.name_orig}:{int(event_time.timestamp())}"),
                    event_time,
                    str(payload.get("txn_type", event.txn_type)),
                    float(payload["amount"]),
                    event.name_orig,
                    str(payload.get("counterparty", event.name_dest)),
                )
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            continue
    return history


def load_recent_receiver_events(
    redis_client: redis.Redis,
    event: TransactionEvent,
    config: PipelineConfig,
) -> list[TransactionEvent]:
    history_key = f"receiver_history:{event.name_dest}"
    current_ts = int(event.event_time.timestamp())
    lower_bound = current_ts - config.fan_in_window_seconds
    try:
        redis_client.zremrangebyscore(history_key, 0, lower_bound - 1)
        members = redis_client.zrangebyscore(history_key, lower_bound, current_ts)
    except redis.RedisError:
        return []

    history: list[TransactionEvent] = []
    for raw_member in members:
        try:
            payload = json.loads(raw_member)
            event_time = datetime.fromisoformat(payload["event_time"])
            history.append(
                build_transaction_stub(
                    event,
                    payload.get("event_id", f"history:{event.name_dest}:{int(event_time.timestamp())}"),
                    event_time,
                    str(payload.get("txn_type", event.txn_type)),
                    float(payload["amount"]),
                    str(payload.get("counterparty", event.name_orig)),
                    event.name_dest,
                )
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            continue
    return history


def load_recent_inbound_events(
    redis_client: redis.Redis,
    event: TransactionEvent,
    config: PipelineConfig,
) -> list[TransactionEvent]:
    history_key = f"inbound_history:{event.name_orig}"
    current_ts = int(event.event_time.timestamp())
    lower_bound = current_ts - config.cashout_after_inbound_window_seconds
    try:
        redis_client.zremrangebyscore(history_key, 0, lower_bound - 1)
        members = redis_client.zrangebyscore(history_key, lower_bound, current_ts)
    except redis.RedisError:
        return []

    history: list[TransactionEvent] = []
    for raw_member in members:
        try:
            payload = json.loads(raw_member)
            event_time = datetime.fromisoformat(payload["event_time"])
            history.append(
                build_transaction_stub(
                    event,
                    payload.get("event_id", f"history:inbound:{event.name_orig}:{int(event_time.timestamp())}"),
                    event_time,
                    str(payload.get("txn_type", "TRANSFER")),
                    float(payload["amount"]),
                    str(payload.get("counterparty", event.name_dest)),
                    event.name_orig,
                )
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            continue
    return history


def load_known_counterparties(redis_client: redis.Redis, account_id: str) -> set[str]:
    try:
        members = redis_client.smembers(f"counterparties:{account_id}")
    except redis.RedisError:
        return set()
    return {member.decode("utf-8") if isinstance(member, bytes) else str(member) for member in members}


def append_sender_history(redis_client: redis.Redis, event: TransactionEvent, config: PipelineConfig) -> None:
    history_key = f"sender_history:{event.name_orig}"
    payload = json.dumps(
        {
            "event_id": event.event_id,
            "event_time": event.event_time.isoformat(),
            "txn_type": event.txn_type,
            "amount": event.amount,
            "counterparty": event.name_dest,
        },
        ensure_ascii=False,
    )
    try:
        redis_client.zadd(history_key, {payload: int(event.event_time.timestamp())})
        redis_client.expire(history_key, max(config.rapid_outflow_window_seconds * 6, 3600))
    except redis.RedisError:
        return


def append_receiver_history(redis_client: redis.Redis, event: TransactionEvent, config: PipelineConfig) -> None:
    history_key = f"receiver_history:{event.name_dest}"
    payload = json.dumps(
        {
            "event_id": event.event_id,
            "event_time": event.event_time.isoformat(),
            "txn_type": event.txn_type,
            "amount": event.amount,
            "counterparty": event.name_orig,
        },
        ensure_ascii=False,
    )
    try:
        redis_client.zadd(history_key, {payload: int(event.event_time.timestamp())})
        redis_client.expire(history_key, max(config.fan_in_window_seconds * 6, 3600))
    except redis.RedisError:
        return


def append_inbound_history(redis_client: redis.Redis, event: TransactionEvent, config: PipelineConfig) -> None:
    if event.txn_type not in {"TRANSFER", "CASH_IN"}:
        return
    history_key = f"inbound_history:{event.name_dest}"
    payload = json.dumps(
        {
            "event_id": event.event_id,
            "event_time": event.event_time.isoformat(),
            "txn_type": event.txn_type,
            "amount": event.amount,
            "counterparty": event.name_orig,
        },
        ensure_ascii=False,
    )
    try:
        redis_client.zadd(history_key, {payload: int(event.event_time.timestamp())})
        redis_client.expire(history_key, max(config.cashout_after_inbound_window_seconds * 6, 3600))
    except redis.RedisError:
        return


def append_counterparty_history(redis_client: redis.Redis, event: TransactionEvent) -> None:
    try:
        redis_client.sadd(f"counterparties:{event.name_orig}", event.name_dest)
        redis_client.expire(f"counterparties:{event.name_orig}", 86400 * 30)
    except redis.RedisError:
        return


def persist_transaction(event: TransactionEvent, risk_score: float) -> None:
    statements = get_prepared_statements()
    get_cassandra_session().execute(
        statements["insert_transaction"],
        (
            event.event_time.date(),
            to_utc_naive(event.event_time),
            event.event_id,
            event.name_orig,
            event.name_dest,
            event.txn_type,
            event.amount,
            event.oldbalance_org,
            event.newbalance_orig,
            event.oldbalance_dest,
            event.newbalance_dest,
            event.is_fraud,
            risk_score,
        ),
    )


def persist_prediction(prediction: PredictionRecord) -> None:
    statements = get_prepared_statements()
    get_cassandra_session().execute(
        statements["insert_prediction"],
        (
            prediction.event_time.date(),
            to_utc_naive(prediction.event_time),
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


def persist_alert(event: TransactionEvent, alert_payload: dict[str, Any]) -> None:
    statements = get_prepared_statements()
    get_cassandra_session().execute(
        statements["insert_alert"],
        (
            event.name_orig,
            event.event_time.date(),
            to_utc_naive(event.event_time),
            alert_payload["alert_id"],
            event.event_id,
            event.name_dest,
            event.txn_type,
            event.amount,
            alert_payload["risk_score"],
            alert_payload["severity"],
            alert_payload["ml_score"],
            alert_payload["ml_model_version"],
            alert_payload["triggered_rules"],
        ),
    )


def persist_account_states(event: TransactionEvent) -> None:
    statements = get_prepared_statements()
    event_ts = to_utc_naive(event.event_time)
    session = get_cassandra_session()
    session.execute(
        statements["insert_account_state"],
        (
            event.name_orig,
            event_ts,
            f"{event.event_id}:sender",
            event.event_id,
            "sender",
            event.step,
            event.oldbalance_org,
            event.newbalance_orig,
        ),
    )
    session.execute(
        statements["insert_account_state"],
        (
            event.name_dest,
            event_ts,
            f"{event.event_id}:receiver",
            event.event_id,
            "receiver",
            event.step,
            event.oldbalance_dest,
            event.newbalance_dest,
        ),
    )


def persist_metric(metric: WindowMetric, window_type: str) -> None:
    statements = get_prepared_statements()
    get_cassandra_session().execute(
        statements["insert_metric"],
        (
            window_type,
            metric.window_start.date(),
            to_utc_naive(metric.window_start),
            to_utc_naive(metric.window_end),
            metric.event_count,
            metric.fraud_count,
            metric.total_amount,
            metric.fraud_rate,
        ),
    )


def extract_benchmark_run_id(event_id: str) -> str | None:
    if not event_id.startswith(BENCHMARK_EVENT_PREFIX):
        return None
    parts = event_id.split(":", 2)
    if len(parts) != 3 or not parts[1]:
        return None
    return parts[1]


def persist_benchmark_stage_timing(
    benchmark_run_id: str,
    event_id: str,
    spark_seen_at: datetime,
    cassandra_persisted_at: datetime,
) -> None:
    statements = get_prepared_statements()
    get_cassandra_session().execute(
        statements["insert_benchmark_stage_timing"],
        (
            benchmark_run_id,
            event_id,
            to_utc_naive(spark_seen_at),
            to_utc_naive(cassandra_persisted_at),
        ),
    )


def publish_alert_once(alert_key: str, payload: dict[str, Any]) -> None:
    producer = get_kafka_producer()
    redis_client = get_redis_client()
    dedupe_key = f"published_alert:{get_pipeline_run_id()}:{alert_key}"
    should_publish = False
    try:
        should_publish = bool(redis_client.set(dedupe_key, "1", ex=86400, nx=True))
    except redis.RedisError:
        should_publish = True
    if should_publish:
        producer.send(FRAUD_ALERTS_TOPIC, key=alert_key, value=payload)


def cache_alert_payload(alert_key: str, payload: dict[str, Any]) -> None:
    try:
        get_redis_client().setex(f"fraud_alert:{alert_key}", 86400, json.dumps(payload, ensure_ascii=False))
    except redis.RedisError:
        return


def process_integrated_batch(batch_df: DataFrame, batch_id: int, query_name: str) -> None:
    if batch_df.limit(1).count() == 0 or batch_already_processed(query_name, batch_id):
        return

    rule_state = get_runtime_rule_state_cached()
    config = config_from_rule_state(rule_state)
    engine = RuleEngine(config)
    producer = get_kafka_producer()
    watchlisted_accounts = set(rule_state.watchlisted_accounts)
    in_batch_sender_history: dict[str, list[TransactionEvent]] = {}
    in_batch_receiver_history: dict[str, list[TransactionEvent]] = {}
    in_batch_inbound_history: dict[str, list[TransactionEvent]] = {}
    in_batch_counterparties: dict[str, set[str]] = {}
    processed_count = 0
    alert_count = 0

    ordered_batch = batch_df.orderBy("event_time")
    for row in ordered_batch.toLocalIterator():
        payload = row.asDict(recursive=True)
        event = integrated_payload_to_transaction_event(payload)
        benchmark_run_id = extract_benchmark_run_id(event.event_id)
        spark_seen_at = datetime.now(timezone.utc)
        recent = load_recent_sender_events(get_redis_client(), event, config)
        recent.extend(in_batch_sender_history.get(event.name_orig, []))
        receiver_recent = load_recent_receiver_events(get_redis_client(), event, config)
        receiver_recent.extend(in_batch_receiver_history.get(event.name_dest, []))
        inbound_recent = load_recent_inbound_events(get_redis_client(), event, config)
        inbound_recent.extend(in_batch_inbound_history.get(event.name_orig, []))
        known_counterparties = load_known_counterparties(get_redis_client(), event.name_orig)
        known_counterparties.update(in_batch_counterparties.get(event.name_orig, set()))
        decision = engine.evaluate(
            event,
            recent_sender_events=recent,
            recent_receiver_events=receiver_recent,
            recent_inbound_events=inbound_recent,
            known_counterparties=known_counterparties,
            watchlisted_accounts=watchlisted_accounts,
        )
        prediction = prediction_record_from_decision(event, decision)
        persist_transaction(event, decision.risk_score)
        persist_prediction(prediction)
        persist_account_states(event)
        if decision.is_alert:
            alert_payload = fraud_decision_to_dict(event, decision)
            persist_alert(event, alert_payload)
            publish_alert_once(event.event_id, alert_payload)
            cache_alert_payload(event.event_id, alert_payload)
            alert_count += 1
        append_sender_history(get_redis_client(), event, config)
        append_receiver_history(get_redis_client(), event, config)
        append_inbound_history(get_redis_client(), event, config)
        append_counterparty_history(get_redis_client(), event)
        cassandra_persisted_at = datetime.now(timezone.utc)
        if benchmark_run_id:
            persist_benchmark_stage_timing(
                benchmark_run_id=str(benchmark_run_id),
                event_id=event.event_id,
                spark_seen_at=spark_seen_at,
                cassandra_persisted_at=cassandra_persisted_at,
            )
        in_batch_sender_history.setdefault(event.name_orig, []).append(event)
        in_batch_receiver_history.setdefault(event.name_dest, []).append(event)
        if event.txn_type in {"TRANSFER", "CASH_IN"}:
            in_batch_inbound_history.setdefault(event.name_dest, []).append(event)
        in_batch_counterparties.setdefault(event.name_orig, set()).add(event.name_dest)
        processed_count += 1

    producer.flush()
    mark_batch_processed(query_name, batch_id)
    print(f"[{query_name}] batch={batch_id} processed={processed_count} alerts={alert_count}")


def write_window_metrics_batch(window_type: str, batch_df: DataFrame, batch_id: int, query_name: str) -> None:
    if batch_df.limit(1).count() == 0 or batch_already_processed(query_name, batch_id):
        return

    producer = get_kafka_producer()
    metric_count = 0
    for row in batch_df.toLocalIterator():
        metric = WindowMetric(
            window_start=row["window_start"].replace(tzinfo=timezone.utc)
            if row["window_start"].tzinfo is None
            else row["window_start"],
            window_end=row["window_end"].replace(tzinfo=timezone.utc)
            if row["window_end"].tzinfo is None
            else row["window_end"],
            event_count=int(row["event_count"]),
            fraud_count=int(row["fraud_count"]),
            total_amount=float(row["total_amount"]),
            fraud_rate=float(row["fraud_rate"]),
        )
        payload = window_metric_to_dict(metric, window_type)
        metric_key = f"{window_type}:{metric.window_start.isoformat()}"
        persist_metric(metric, window_type)
        producer.send(METRICS_WINDOWED_TOPIC, key=metric_key, value=payload)
        metric_count += 1
    producer.flush()
    mark_batch_processed(query_name, batch_id)
    print(f"[{query_name}] batch={batch_id} windows={metric_count}")


def start_query(
    frame: DataFrame,
    query_name: str,
    checkpoint_suffix: str,
    foreach_batch=None,
    output_mode: str = "append",
):
    writer = (
        frame.writeStream.queryName(query_name)
        .outputMode(output_mode)
        .option("checkpointLocation", os.path.join(CHECKPOINT_ROOT, checkpoint_suffix))
    )
    if foreach_batch is not None:
        return writer.trigger(processingTime="2 seconds").foreachBatch(foreach_batch).start()
    return (
        writer.format("kafka")
        .trigger(processingTime="2 seconds")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", PIPELINE_DEAD_LETTER_TOPIC)
        .start()
    )


def main() -> None:
    ensure_schema(CASSANDRA_HOST, CASSANDRA_PORT, CASSANDRA_KEYSPACE)
    get_pipeline_run_id()
    warm_rule_state_cache()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    tx_valid, tx_invalid = decode_json_stream(
        create_kafka_stream(spark, TRANSACTION_TOPIC),
        transaction_schema(),
        required_fields=[
            "event_id",
            "event_time",
            "producer_ts",
            "step",
            "type",
            "amount",
            "nameOrig",
            "nameDest",
            "isFraud",
        ],
        timestamp_fields=["event_time", "producer_ts"],
    )
    sender_valid, sender_invalid = decode_json_stream(
        create_kafka_stream(spark, SENDER_STATE_TOPIC),
        sender_state_schema(),
        required_fields=[
            "event_id",
            "source_event_id",
            "event_time",
            "step",
            "nameOrig",
            "oldbalanceOrg",
            "newbalanceOrig",
        ],
        timestamp_fields=["event_time"],
    )
    receiver_valid, receiver_invalid = decode_json_stream(
        create_kafka_stream(spark, RECEIVER_STATE_TOPIC),
        receiver_state_schema(),
        required_fields=[
            "event_id",
            "source_event_id",
            "event_time",
            "step",
            "nameDest",
            "oldbalanceDest",
            "newbalanceDest",
        ],
        timestamp_fields=["event_time"],
    )

    integrated_valid, integration_invalid = build_integrated_stream(tx_valid, sender_valid, receiver_valid)
    dead_letters = (
        tx_invalid.unionByName(sender_invalid).unionByName(receiver_invalid).unionByName(integration_invalid)
    )
    tumbling_metrics = build_window_metrics(integrated_valid, "tumbling", TUMBLING_WINDOW)
    sliding_metrics = build_window_metrics(integrated_valid, "sliding", SLIDING_WINDOW, SLIDE_INTERVAL)
    integrated_query_name = scoped_query_name_for_run("integrated_fraud_pipeline")
    tumbling_query_name = scoped_query_name_for_run("tumbling_window_metrics")
    sliding_query_name = scoped_query_name_for_run("sliding_window_metrics")

    queries = [
        start_query(dead_letters, "pipeline_dead_letters", "dead_letters_v3"),
        start_query(
            integrated_valid,
            integrated_query_name,
            "integrated_pipeline_v3",
            foreach_batch=lambda df, batch_id: process_integrated_batch(df, batch_id, integrated_query_name),
        ),
        start_query(
            tumbling_metrics,
            tumbling_query_name,
            "tumbling_metrics_v3",
            foreach_batch=lambda df, batch_id: write_window_metrics_batch("tumbling", df, batch_id, tumbling_query_name),
            output_mode="append",
        ),
        start_query(
            sliding_metrics,
            sliding_query_name,
            "sliding_metrics_v3",
            foreach_batch=lambda df, batch_id: write_window_metrics_batch("sliding", df, batch_id, sliding_query_name),
            output_mode="append",
        ),
    ]
    for query in queries:
        query.awaitTermination()


if __name__ == "__main__":
    main()
