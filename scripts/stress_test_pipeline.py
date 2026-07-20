#!/usr/bin/env python3
import argparse
import json
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer
from concurrent.futures import ThreadPoolExecutor

# Topics
TRANSACTION_TOPIC = "transaction_topic"
SENDER_STATE_TOPIC = "sender_state_topic"
RECEIVER_STATE_TOPIC = "receiver_state_topic"

def create_payload(event_id, step, tx_type, amount, name_orig, name_dest):
    event_time = datetime.now(timezone.utc).isoformat()
    
    tx = {
        "event_id": event_id,
        "event_time": event_time,
        "producer_ts": event_time,
        "step": step,
        "type": tx_type,
        "amount": amount,
        "nameOrig": name_orig,
        "nameDest": name_dest,
        "isFraud": 0,
        "schema_version": 1
    }
    
    sender = {
        "event_id": f"sender_{event_id}",
        "source_event_id": event_id,
        "event_time": event_time,
        "step": step,
        "nameOrig": name_orig,
        "oldbalanceOrg": 1000000.0,
        "newbalanceOrig": 1000000.0 - amount
    }
    
    receiver = {
        "event_id": f"receiver_{event_id}",
        "source_event_id": event_id,
        "event_time": event_time,
        "step": step,
        "nameDest": name_dest,
        "oldbalanceDest": 0.0,
        "newbalanceDest": amount
    }
    
    return tx, sender, receiver

def produce_load(bootstrap_servers, target_eps, duration_sec):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks=1,
        linger_ms=50,
        batch_size=65536,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    total_events = target_eps * duration_sec
    print(f"Starting stress test: {target_eps} EPS for {duration_sec}s (Total: {total_events} events)")
    
    start_time = time.time()
    sent = 0
    
    while time.time() - start_time < duration_sec:
        batch_start = time.time()
        for _ in range(target_eps):
            event_id = str(uuid.uuid4())
            tx, sender, receiver = create_payload(event_id, 1, "TRANSFER", 100.0, "C123", "C456")
            
            producer.send(TRANSACTION_TOPIC, tx)
            producer.send(SENDER_STATE_TOPIC, sender)
            producer.send(RECEIVER_STATE_TOPIC, receiver)
            sent += 1
            
        # Rate limiting
        elapsed = time.time() - batch_start
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)
            
        print(f"Sent {sent} events... (Actual EPS: {sent / (time.time() - start_time):.2f})")

    producer.flush()
    print(f"Stress test complete. Total sent: {sent}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--brokers", default="localhost:9092")
    parser.add_argument("--eps", type=int, default=1000)
    parser.add_argument("--duration", type=int, default=60)
    args = parser.parse_args()
    
    produce_load(args.brokers, args.eps, args.duration)
