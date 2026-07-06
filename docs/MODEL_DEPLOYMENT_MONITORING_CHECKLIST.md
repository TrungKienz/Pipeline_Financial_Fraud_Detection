# Model Deployment And Monitoring Checklist

Checklist nay chuyen tu danh sach gap thanh ke hoach implement theo file cu the de theo doi trong repo.

## Phase 1: Inference API

- [x] `api/app.py`
  - Tao FastAPI app
  - Them `GET /health`
  - Them `POST /score`
  - Da them `POST /score/batch`
- [x] `api/schemas.py`
  - Dinh nghia `ScoreRequest`
  - Dinh nghia `ScoreResponse`
  - Da co schema batch
- [x] `api/service.py`
  - Chuyen request thanh `TransactionEvent`
  - Goi `RuleEngine.evaluate(...)`
  - Tra ve `ml_score`, `risk_score`, `severity`, `triggered_rules`, `ml_model_version`, `is_alert`
- [x] `api/Dockerfile`
  - Build inference service doc lap
  - Copy `fraud_pipeline/`, `model/`, `api/`
  - Chay bang `uvicorn`
- [x] `api/requirements.txt`
  - Them `fastapi`, `uvicorn`
  - Them dependencies runtime can thiet
- [x] `docker-compose.yml`
  - Them service `api`
  - Expose port `8000:8000`
  - Da them env `FRAUD_MODEL_TYPE=v1`
- [x] `tests/test_api.py`
  - Test health endpoint
  - Test score endpoint voi payload hop le
  - Test payload thieu field
  - Test fallback khi model khong load

Ghi chu Phase 1:

- Da cap nhat them `requirements-local.txt` va `requirements-local-pinned.txt` de ho tro chay/test API local.
- Da verify `python -m pytest tests/test_api.py` voi ket qua `4 passed`.
- API dang default sang `FRAUD_MODEL_TYPE=v1` vi artifact `xgb` hien co van de tuong thich khi suy luan trong moi truong hien tai.
- Health endpoint va score endpoint da chay duoc end-to-end.

## Phase 2: Shared Scoring Logic

- [x] `fraud_pipeline/rules.py`
  - Giu scoring logic dung chung cho Spark va API
  - Khong can tach lai logic rule, API tiep tuc dung truc tiep `RuleEngine.evaluate(...)`
- [x] `fraud_pipeline/models.py`
  - Optional: them dataclass cho monitoring nhu `PredictionRecord`, `ReviewRecord`
  - Da them `PredictionRecord`
- [x] `fraud_pipeline/serialization.py`
  - Them serializer cho prediction log va review log neu can
  - Da them `prediction_record_from_decision(...)`
  - Da them `prediction_record_to_dict(...)`

Ghi chu Phase 2:

- API va alert payload hien tai da cung dung chung prediction contract qua `PredictionRecord`.
- `fraud_decision_to_dict(...)` da delegate sang helper chung thay vi map field rieng.
- `api/service.py` da duoc refactor de dung shared prediction contract moi.
- Da bo sung test trong `tests/test_serialization.py`.
- Da verify `python -m pytest tests/test_api.py tests/test_serialization.py` voi ket qua `7 passed`.
- Van con residual risk voi artifact `xgb` cu: model load duoc nhung suy luan co the loi do version mismatch khi unpickle.

## Phase 3: Streamlit Review Queue

- [x] `dashboard/streamlit/app.py`
  - Tach giao dien thanh cac tab:
  - `Live Alerts`
  - `Review Queue`
  - `Case Details`
  - `Monitoring`
  - Trong `Review Queue` can co:
  - Danh sach alert chua review
  - Filter severity / rule / time
  - Chon case de review
  - Action: `Mark Fraud`, `Mark Legit`, `Escalate`, `Needs More Info`
  - Form luu `review_status`, `review_label`, `reviewer`, `notes`
- [x] `dashboard/streamlit/requirements.txt`
  - Them package moi neu Streamlit goi API hoac render monitoring
  - Khong can them package moi o Phase 3
- [x] `dashboard/streamlit/Dockerfile`
  - Dong bo dependency moi
  - Khong can chinh Dockerfile vi requirements khong doi

Ghi chu Phase 3:

- Da refactor dashboard thanh 4 tab: `Live Alerts`, `Review Queue`, `Case Details`, `Monitoring`.
- Review queue hien tai la demo workflow, dung `st.session_state` de luu thao tac analyst trong phien Streamlit.
- Da them filter queue, chon case, action nhanh, va form cap nhat review.
- `Case Details` hien thi thong tin alert, model score, triggered rules, va review snapshot.
- `Monitoring` tab hien tai theo doi queue workflow o muc demo; model monitoring thuc su se tiep tuc o cac phase sau.
- Da verify `python -m py_compile dashboard/streamlit/app.py` va load module app thanh cong.

## Phase 4: Prediction Logging And Feedback Storage

- [x] `fraud_pipeline/cassandra_schema.py`
  - Da them bang `alert_reviews`
  - Da them bang `model_predictions_by_day`
- [x] `spark-app/stream_job.py`
  - Them `insert_prediction` prepared statement
  - Them `persist_prediction(...)`
  - Luu prediction cho moi transaction, khong chi alert
- [x] `api/service.py`
  - Neu API score doc lap thi luu prediction log thong nhat voi Spark path
  - Logging duoc bat/tat bang env `API_PREDICTION_LOGGING_ENABLED`

Ghi chu Phase 4:

- Da them bang `model_predictions_by_day` de luu serving predictions cho model monitoring.
- Spark path hien da persist prediction cho moi transaction thong qua `PredictionRecord`.
- API path da co hook logging tuy chon khi chay cung Cassandra.
- `alert_reviews` da co schema de phuc vu feedback/review persistence.
- Da verify `python -m pytest tests/test_api.py tests/test_serialization.py` voi ket qua `7 passed`.
- Da verify `python -m py_compile fraud_pipeline/cassandra_schema.py spark-app/stream_job.py api/service.py`.

## Phase 5: Review Queue Data Access

- [x] `dashboard/streamlit/app.py`
  - Them `load_unreviewed_alerts()`
  - Them `load_alert_reviews()`
  - Them `save_alert_review(...)`
  - Them `merge_alerts_with_reviews(...)`
- [x] `tests/test_serialization.py`
  - Them test cho serializer review/prediction neu bo sung
- [ ] `tests/test_integration.py`
  - Them test flow luu prediction/review neu co mock phu hop

Ghi chu Phase 5:

- Review queue hien da doc/ghi persistence that tu Cassandra table `alert_reviews`.
- `dashboard/streamlit/app.py` da co du 4 ham duoc checklist yeu cau: `load_unreviewed_alerts`, `load_alert_reviews`, `save_alert_review`, `merge_alerts_with_reviews`.
- Analyst actions va form review hien da luu xuong Cassandra thay vi `st.session_state` only.
- Da verify `python -m py_compile fraud_pipeline/cassandra_schema.py dashboard/streamlit/app.py`.
- Da verify app load duoc va expose cac helper review queue can thiet.

## Phase 6: Drift Monitoring Baseline

- [x] `monitoring/model/`
  - Tao thu muc rieng cho model monitoring
- [x] `monitoring/model/reference_builder.py`
  - Tao baseline tu training/reference data
  - Output `monitoring/reference/reference_summary.json`
  - Da luu reference dataset CSV
- [x] `monitoring/model/drift_report.py`
  - Doc reference data va serving data gan day
  - Hien tai sinh drift report HTML/JSON bang custom report nhe, khong phu thuoc bat buoc vao Evidently runtime
  - Output `monitoring/reports/drift_report.html`
  - Output `monitoring/reports/drift_report.json`
- [x] `monitoring/model/requirements.txt`
  - Them `evidently`, `pandas`, `numpy`, `cassandra-driver`
- [ ] `requirements-local.txt`
  - Can nhac them dependency monitoring/API neu muon chay local chung

Ghi chu Phase 6:

- Da tao `monitoring/model/reference_builder.py` de sinh baseline tu `model/test_set.csv`.
- Da tao `monitoring/model/drift_report.py` de so sanh reference voi serving data tu Cassandra hoac CSV fallback.
- Da sinh thanh cong:
  - `monitoring/reference/reference_dataset.csv`
  - `monitoring/reference/reference_summary.json`
  - `monitoring/reports/drift_report.json`
  - `monitoring/reports/drift_report.html`
- Da verify `python monitoring/model/reference_builder.py --max-rows 200`.
- Da verify `python monitoring/model/drift_report.py --reference-csv monitoring/reference/reference_dataset.csv --serving-csv monitoring/reference/reference_dataset.csv`.
- Residual risk: artifact ML `xgb` cu van co warning/version mismatch, nen baseline scoring hien dang van hanh theo fallback an toan khi can.

## Phase 7: Rolling Performance Monitoring

- [x] `monitoring/model/performance_report.py`
  - Join prediction log voi review labels
  - Tinh `precision`, `recall`, `f1`, `false_positive_rate`
  - Tinh theo rolling windows `1d`, `7d`, `30d`
- [x] `monitoring/model/metrics_store.py`
  - Helper load predictions tu Cassandra
  - Helper load reviews/labels tu Cassandra
  - Chuyen thanh DataFrame de tinh toan
- [ ] `fraud_pipeline/models.py`
  - Optional: them dataclass cho `ModelPerformanceSnapshot`, `DriftSummary`
- [ ] `fraud_pipeline/serialization.py`
  - Optional: them serializer cho cac snapshot monitoring

Ghi chu Phase 7:

- Da tao `monitoring/model/metrics_store.py` de load prediction log va review labels tu Cassandra hoac CSV fallback.
- Da tao `monitoring/model/performance_report.py` de tinh metrics over time tren rolling windows `1d`, `7d`, `30d`.
- Da sinh thanh cong `monitoring/reports/performance_report.json`.
- Report hien theo doi:
  - `precision`
  - `recall`
  - `f1`
  - `false_positive_rate`
  - `label_coverage`
  - `label_source_breakdown`
- Da verify `python -m pytest tests/test_monitoring_performance.py tests/test_api.py tests/test_serialization.py` voi ket qua `10 passed`.
- Da verify `python -m py_compile monitoring/model/metrics_store.py monitoring/model/performance_report.py`.
- Da verify end-to-end bang CSV mau va ghi ra `monitoring/reports/performance_report.json`.
- Residual risk: neu he thong moi chi co review cho alert cases thi recall thuc te co the optimistic do false negatives chua duoc label day du.

## Phase 8: Retraining Trigger

- [x] `monitoring/model/retraining_policy.json`
  - Dinh nghia nguong trigger:
  - `psi_amount_threshold`
  - `feature_drift_ratio_threshold`
  - `precision_7d_min`
  - `recall_7d_min`
  - `alert_rate_change_threshold`
- [x] `monitoring/model/check_retraining_trigger.py`
  - Doc drift report
  - Doc performance report
  - Doc policy json
  - Xuat `retrain_required` va `reasons`
- [x] `monitoring/reports/retraining_decision.json`
  - Luu ket qua de dashboard/doc lai

Ghi chu Phase 8:

- Da them `monitoring/model/retraining_policy.json` voi cac nguong drift/performance co the dieu chinh.
- Da them `monitoring/model/check_retraining_trigger.py` de tong hop drift report va performance report.
- Da sinh thanh cong `monitoring/reports/retraining_decision.json`.
- Decision hien tai la `retrain_required: false` vi drift = 0 va he thong dang danh gia conservative khi sample 7d chua du lon.
- Da verify `python -m py_compile monitoring/model/check_retraining_trigger.py`.
- Da verify `python -m pytest tests/test_retraining_trigger.py tests/test_monitoring_performance.py` voi ket qua `5 passed`.
- Da verify `python monitoring/model/check_retraining_trigger.py` va ghi output JSON thanh cong.

## Phase 9: Monitoring UI

- [x] `dashboard/streamlit/app.py`
  - Them tab `Monitoring`
  - Hien thi drift summary
  - Hien thi top drifting features
  - Hien thi score distribution
  - Hien thi rolling precision/recall
  - Hien thi retraining recommendation
- [ ] `monitoring/grafana/provisioning/dashboards/fraud-dashboard.json`
  - Optional: mo rong them panel alert rate / prediction volume

Ghi chu Phase 9:

- Tab `Monitoring` hien da doc truc tiep tu:
  - `monitoring/reports/drift_report.json`
  - `monitoring/reports/performance_report.json`
  - `monitoring/reports/retraining_decision.json`
- Dashboard hien thi:
  - drift summary
  - rolling performance metrics
  - score distribution snapshot
  - retraining decision va warnings
  - label source / coverage context
  - review workflow context de doi chieu voi model metrics
- Da verify `python -m py_compile dashboard/streamlit/app.py`.
- Da verify app load duoc va expose `load_report(...)` cung `render_monitoring_tab(...)`.

## Phase 10: Deployment Artifacts And Docs

- [x] `render.yaml`
  - Cau hinh deploy `api` len Render
- [x] `docs/MODEL_DEPLOYMENT.md`
  - Huong dan local run API
  - Huong dan local run Streamlit
  - Huong dan chay monitoring scripts
  - Huong dan deploy cloud
- [x] `README.md`
  - Cap nhat architecture voi API, review queue, model monitoring
  - Cap nhat runbook va endpoints moi
- [ ] `dashboard/streamlit/README.md`
  - Optional: huong dan deploy Streamlit demo len Hugging Face Spaces hoac nen tang tuong duong

Ghi chu Phase 10:

- Da them `docs/MODEL_DEPLOYMENT.md` lam runbook tong hop cho API, Streamlit, monitoring scripts, va deploy notes.
- Da them `render.yaml` toi thieu de deploy FastAPI service len Render free tier.
- Da cap nhat `README.md` de phan anh:
  - module `api`
  - module `monitoring`
  - buoc chay API
  - buoc chay model monitoring scripts
- Da cap nhat `docs/README.md` de link toi tai lieu moi.
- Da verify `render.yaml` parse duoc va `api/app.py` cung `monitoring/model/check_retraining_trigger.py` compile duoc.

## Thu Tu Implement Khuyen Nghi

- [x] Buoc 1: `api/app.py`, `api/schemas.py`, `api/service.py`
- [x] Buoc 2: `api/Dockerfile`, `api/requirements.txt`, `docker-compose.yml`
- [x] Buoc 3: `tests/test_api.py`
- [x] Buoc 4: `fraud_pipeline/cassandra_schema.py`
- [x] Buoc 5: `spark-app/stream_job.py`
- [x] Buoc 6: `dashboard/streamlit/app.py` cho review queue
- [x] Buoc 7: `monitoring/model/reference_builder.py`
- [x] Buoc 8: `monitoring/model/drift_report.py`
- [x] Buoc 9: `monitoring/model/performance_report.py`
- [x] Buoc 10: `monitoring/model/retraining_policy.json`, `monitoring/model/check_retraining_trigger.py`
- [x] Buoc 11: `dashboard/streamlit/app.py` cho monitoring tab
- [x] Buoc 12: `docs/MODEL_DEPLOYMENT.md`, `README.md`, `render.yaml`

## Scope Toi Thieu De Dat Yeu Cau 6 Va 7

- [x] `api/app.py`
- [x] `api/schemas.py`
- [x] `api/service.py`
- [x] `api/Dockerfile`
- [x] `docker-compose.yml`
- [x] `tests/test_api.py`
- [x] `fraud_pipeline/models.py`
- [x] `fraud_pipeline/serialization.py`
- [x] `fraud_pipeline/cassandra_schema.py`
- [x] `spark-app/stream_job.py`
- [x] `dashboard/streamlit/app.py`
- [x] `monitoring/model/reference_builder.py`
- [x] `monitoring/model/drift_report.py`
- [x] `monitoring/model/performance_report.py`
- [x] `monitoring/model/retraining_policy.json`
- [x] `monitoring/model/check_retraining_trigger.py`
- [x] `docs/MODEL_DEPLOYMENT.md`

## Diem Nghen Lon Nhat Hien Tai

- [x] Thieu `API layer`
- [x] Thieu `review feedback labels`
- [x] Thieu `reference baseline`
- [x] Thieu `model-specific monitoring`, hien moi co `system monitoring`
