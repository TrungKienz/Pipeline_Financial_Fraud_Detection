"""
Create a balanced dataset from the FULL PaySim dataset (6.3M rows, 8213 fraud).
Equal number of fraud and non-fraud transactions, then split into logical source CSVs.
"""
import csv
import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from fraud_pipeline import PipelineConfig, split_integrated_csv_to_logical_sources

FULL_CSV = r"C:\Users\Asus\Desktop\archive\PS_20174392719_1491204439457_log.csv"
BALANCED_CSV = ROOT / "model" / "pipeline_test_set.csv"
LOGICAL_SOURCES_DIR = ROOT / "Data" / "logical_sources"
SEED = 42

random.seed(SEED)

print("=" * 60)
print("STEP 1: Reading full PaySim dataset")
print("=" * 60)

fraud_rows = []
nonfraud_rows = []

with open(FULL_CSV, "r", newline="") as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames
    for row in reader:
        if int(row["isFraud"]) == 1:
            fraud_rows.append(row)
        else:
            nonfraud_rows.append(row)

print(f"  Total rows:       {len(fraud_rows) + len(nonfraud_rows):,}")
print(f"  Fraud rows:       {len(fraud_rows):,}")
print(f"  Non-fraud rows:   {len(nonfraud_rows):,}")

n_fraud = len(fraud_rows)
sample_nonfraud = random.sample(nonfraud_rows, n_fraud)

balanced_rows = fraud_rows + sample_nonfraud
random.shuffle(balanced_rows)

print(f"\n  Balanced: {len(balanced_rows):,} rows ({n_fraud:,} fraud + {n_fraud:,} non-fraud)")

print("\n" + "=" * 60)
print("STEP 2: Writing balanced pipeline_test_set.csv")
print("=" * 60)

with open(BALANCED_CSV, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(balanced_rows)

fraud_count = sum(1 for r in balanced_rows if int(r["isFraud"]) == 1)
print(f"  Written {len(balanced_rows):,} rows to {BALANCED_CSV.name}")
print(f"  Fraud: {fraud_count:,}, Non-fraud: {len(balanced_rows) - fraud_count:,}")

print("\n" + "=" * 60)
print("STEP 3: Splitting into logical source CSVs")
print("=" * 60)

config = PipelineConfig()
summary = split_integrated_csv_to_logical_sources(
    str(BALANCED_CSV),
    str(LOGICAL_SOURCES_DIR),
    config=config,
    limit=None,
)

print(f"\n  Event count:       {summary['event_count']:,}")
print(f"  Transaction CSV:  {summary['transaction_csv']}")
print(f"  Sender state CSV: {summary['sender_state_csv']}")
print(f"  Receiver state CSV: {summary['receiver_state_csv']}")

print("\n" + "=" * 60)
print("STEP 4: Verification")
print("=" * 60)

tx_fraud = 0
tx_total = 0
with open(summary["transaction_csv"], "r", newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        tx_total += 1
        if int(row["isFraud"]) == 1:
            tx_fraud += 1

print(f"  Transaction source: {tx_total:,} rows, {tx_fraud:,} fraud")
print(f"  Balanced: {'YES' if tx_fraud == tx_total - tx_fraud else 'NO - check!'}")

print("\nDone!")
