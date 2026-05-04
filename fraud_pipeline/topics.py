TRANSACTION_TOPIC = "transaction_topic"
SENDER_STATE_TOPIC = "sender_state_topic"
RECEIVER_STATE_TOPIC = "receiver_state_topic"
RISK_RULES_TOPIC = "risk_rules"
FRAUD_ALERTS_TOPIC = "fraud_alerts"
METRICS_WINDOWED_TOPIC = "metrics_windowed"
PIPELINE_DEAD_LETTER_TOPIC = "pipeline_dead_letter"

# Backward-compatible aliases for older code paths and docs.
TRANSACTIONS_RAW_TOPIC = TRANSACTION_TOPIC

ALL_TOPICS = (
    TRANSACTION_TOPIC,
    SENDER_STATE_TOPIC,
    RECEIVER_STATE_TOPIC,
    RISK_RULES_TOPIC,
    FRAUD_ALERTS_TOPIC,
    METRICS_WINDOWED_TOPIC,
    PIPELINE_DEAD_LETTER_TOPIC,
)
