# Fraud Model Training

This directory contains offline ML training and runtime artifacts for fraud scoring.

Train and compare the supported classifiers:

```powershell
python model/train_model.py --csv-path C:\path\to\paysim.csv --model-types all --false-alarm-cost 5
```

The training script compares Logistic Regression, Random Forest, XGBoost, and LightGBM when their dependencies are installed. Thresholds are selected by minimum validation business cost:

```text
Business Cost = sum(amount_i for false negatives) + 5 * false_positives
No-Model Baseline Cost = sum(amount_i for all actual fraud transactions)
Net Cost Savings = No-Model Baseline Cost - Business Cost
Savings Rate = Net Cost Savings / No-Model Baseline Cost
```

Key outputs:

- `model_comparison.json`: per-model metrics, costs, savings, and selected model.
- `selected_model.json`: runtime pointer used when `FRAUD_MODEL_TYPE` is not set.
- `eval_results.json`: selected production model evaluation.
- `fraud_model_<tag>.pkl` and `model_metadata_<tag>.json`: per-model artifacts.
