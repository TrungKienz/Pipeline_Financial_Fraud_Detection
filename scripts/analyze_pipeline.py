import csv
from collections import Counter

fraud_rows = []
nonfraud_rows = []
step_counter = Counter()
type_counter = Counter()

with open('C:\\Users\\Asus\\Desktop\\BigData\\model\\pipeline_test_set.csv', 'r') as f:
    reader = csv.DictReader(f)
    for i, row in enumerate(reader):
        is_fraud = int(row['isFraud'])
        if is_fraud == 1:
            fraud_rows.append((i, row))
        else:
            nonfraud_rows.append((i, row))
        step_counter[row['step']] += 1
        type_counter[row['type']] += 1

print(f'Total rows: {len(fraud_rows) + len(nonfraud_rows)}')
print(f'Fraud (is_fraud=1): {len(fraud_rows)}')
print(f'Non-fraud (is_fraud=0): {len(nonfraud_rows)}')
print()
print('Step distribution:')
for step, cnt in sorted(step_counter.items(), key=lambda x: int(x[0])):
    print(f'  step={step}: {cnt}')
print()
print('Type distribution:')
for t, cnt in type_counter.most_common():
    print(f'  {t}: {cnt}')
print()
print('Fraud rows details:')
for idx, row in fraud_rows:
    print(f'  row={idx}, step={row["step"]}, type={row["type"]}, amount={row["amount"]}, nameOrig={row["nameOrig"]}, nameDest={row["nameDest"]}')
