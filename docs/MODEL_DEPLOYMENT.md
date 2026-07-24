# Model Deployment And Monitoring Runbook

Tai lieu nay tong hop cach chay API scoring, Streamlit review queue, model monitoring scripts, va artifact deploy toi thieu cho project hien tai.

## 1. Thanh Phan Da Co

- `api/`: FastAPI scoring service
- `dashboard/streamlit/`: Streamlit demo + review queue + monitoring dashboard
- `monitoring/model/`: scripts tao baseline, drift report, performance report, retraining trigger
- `render.yaml`: deploy artifact toi thieu cho API

## 2. Chay Local API

### Cach 1: bang Python

```powershell
python -m pip install -r api/requirements.txt
uvicorn api.app:app --host 0.0.0.0 --port 8000
```

### Cach 2: bang Docker Compose

```powershell
docker-compose up -d api
```

### Endpoint chinh

- `GET /health`
- `POST /score`
- `POST /score/batch`

### Vi du request

```powershell
curl -X POST http://localhost:8000/score ^
  -H "Content-Type: application/json" ^
  -d "{\"step\":1,\"type\":\"TRANSFER\",\"amount\":260000,\"nameOrig\":\"C1\",\"oldbalanceOrg\":300000,\"newbalanceOrig\":100,\"nameDest\":\"C2\",\"oldbalanceDest\":1000,\"newbalanceDest\":261000,\"isFraud\":1}"
```

## 3. Chay Local Streamlit

### Cach 1: bang Python

```powershell
python -m pip install -r dashboard/streamlit/requirements.txt
streamlit run dashboard/streamlit/app.py
```

### Cach 2: bang Docker Compose

```powershell
docker-compose up -d streamlit
```

### Giao dien hien tai

- `Live Alerts`
- `Review Queue`
- `Case Details`
- `Monitoring`

## 4. Chay Monitoring Scripts

### Cai dependency monitoring

```powershell
python -m pip install -r monitoring/model/requirements.txt
```

### 4.1. Tao reference baseline

```powershell
python monitoring/model/reference_builder.py --max-rows 5000
```

Output:

- `monitoring/reference/reference_dataset.csv`
- `monitoring/reference/reference_summary.json`

### 4.2. Tao drift report

Tu Cassandra:

```powershell
python monitoring/model/drift_report.py --cassandra-host localhost --cassandra-port 9042 --cassandra-keyspace fraud_detection
```

Tu CSV fallback:

```powershell
python monitoring/model/drift_report.py --reference-csv monitoring/reference/reference_dataset.csv --serving-csv monitoring/reference/reference_dataset.csv
```

Output:

- `monitoring/reports/drift_report.json`
- `monitoring/reports/drift_report.html`

### 4.3. Tao performance report

Tu Cassandra:

```powershell
python monitoring/model/performance_report.py --cassandra-host localhost --cassandra-port 9042 --cassandra-keyspace fraud_detection
```

Output:

- `monitoring/reports/performance_report.json`

### 4.4. Kiem tra retraining trigger

```powershell
python monitoring/model/check_retraining_trigger.py
```

Output:

- `monitoring/reports/retraining_decision.json`

## 5. Bien Moi Truong Quan Trong

### API

- `FRAUD_MODEL_TYPE`
- `API_PREDICTION_LOGGING_ENABLED`
- `CASSANDRA_HOST`
- `CASSANDRA_PORT`
- `CASSANDRA_KEYSPACE`

### Spark App

- `KAFKA_BOOTSTRAP_SERVERS`
- `CASSANDRA_HOST`
- `REDIS_HOST`
- `PIPELINE_STARTING_OFFSETS`

## 6. Render Deployment

Repo da co `render.yaml` toi thieu cho API.

Luu y thuc te:

- Render free tier phu hop nhat de deploy `api` doc lap.
- Khong nen ky vong deploy full local stack Kafka + Spark + Cassandra + Redis len free tier theo dang hien tai.
- Streamlit co the deploy rieng len Hugging Face Spaces hoac nen tang tuong duong.

## 7. Hugging Face Spaces Gọi Y

Neu muon demo Streamlit tren cloud:

- dung `dashboard/streamlit/` lam app source
- cai dependency tu `dashboard/streamlit/requirements.txt`
- cau hinh app start command:

```bash
streamlit run app.py --server.port 7860 --server.address 0.0.0.0
```

Luu y: de giao dien hien day du du lieu, can co nguon Cassandra chua alerts va reviews.

## 8. Tinh Trang Hien Tai

Da hoan thanh:

- API scoring real-time
- Streamlit review queue persistence that
- prediction logging
- baseline drift monitoring
- rolling performance monitoring
- retraining trigger
- monitoring dashboard trong Streamlit

Chua hoan thanh hoan toan:

- artifact deploy cloud cho Streamlit
- tai lieu rollout production chi tiet hon
