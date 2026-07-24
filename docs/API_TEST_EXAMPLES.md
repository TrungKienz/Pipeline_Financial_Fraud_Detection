# API Test Examples

Tai lieu nay tong hop cac vi du test `POST /score`, `POST /score/batch`, va `GET /health` cho `api/` trong project.

## 1. Chay API local

### Cach 1: bang Python

```powershell
python -m pip install -r api/requirements.txt
uvicorn api.app:app --host 0.0.0.0 --port 8000
```

### Cach 2: bang Docker Compose

```powershell
docker-compose up -d api
```

Sau khi chay, API se san sang tai `http://localhost:8000`.

## 2. Test `POST /score` bang curl

Dung `curl.exe` tren Windows PowerShell de tranh xung dot voi alias `curl`.

```powershell
curl.exe -X POST "http://localhost:8000/score" `
  -H "Content-Type: application/json" `
  -d "{\"step\":1,\"type\":\"TRANSFER\",\"amount\":260000,\"nameOrig\":\"C123\",\"oldbalanceOrg\":300000,\"newbalanceOrig\":100,\"nameDest\":\"C456\",\"oldbalanceDest\":1000,\"newbalanceDest\":261000,\"isFraud\":1}"
```

Response mau:

```json
{
  "event_id": "...",
  "is_alert": true,
  "risk_score": 0.93,
  "severity": "high",
  "ml_score": 0.88,
  "ml_model_version": "v1",
  "triggered_rules": ["large_transfer", "balance_drain"]
}
```

## 3. Test `POST /score/batch` bang curl

```powershell
curl.exe -X POST "http://localhost:8000/score/batch" `
  -H "Content-Type: application/json" `
  -d "{\"transactions\":[{\"step\":1,\"type\":\"TRANSFER\",\"amount\":260000,\"nameOrig\":\"C123\",\"oldbalanceOrg\":300000,\"newbalanceOrig\":100,\"nameDest\":\"C456\",\"oldbalanceDest\":1000,\"newbalanceDest\":261000,\"isFraud\":1},{\"step\":2,\"type\":\"CASH_OUT\",\"amount\":5000,\"nameOrig\":\"C789\",\"oldbalanceOrg\":7000,\"newbalanceOrig\":2000,\"nameDest\":\"M111\",\"oldbalanceDest\":0,\"newbalanceDest\":5000,\"isFraud\":0}]}"
```

Response mau:

```json
{
  "predictions": [
    {
      "event_id": "...",
      "is_alert": true,
      "risk_score": 0.93,
      "severity": "high",
      "ml_score": 0.88,
      "ml_model_version": "v1",
      "triggered_rules": ["large_transfer", "balance_drain"]
    },
    {
      "event_id": "...",
      "is_alert": false,
      "risk_score": 0.21,
      "severity": "low",
      "ml_score": 0.18,
      "ml_model_version": "v1",
      "triggered_rules": []
    }
  ]
}
```

## 4. Test bang PowerShell `Invoke-RestMethod`

### `POST /score`

```powershell
$body = @{
  step = 1
  type = "TRANSFER"
  amount = 260000
  nameOrig = "C123"
  oldbalanceOrg = 300000
  newbalanceOrig = 100
  nameDest = "C456"
  oldbalanceDest = 1000
  newbalanceDest = 261000
  isFraud = 1
} | ConvertTo-Json

Invoke-RestMethod -Method Post -Uri "http://localhost:8000/score" -ContentType "application/json" -Body $body
```

### `POST /score/batch`

```powershell
$body = @{
  transactions = @(
    @{
      step = 1
      type = "TRANSFER"
      amount = 260000
      nameOrig = "C123"
      oldbalanceOrg = 300000
      newbalanceOrig = 100
      nameDest = "C456"
      oldbalanceDest = 1000
      newbalanceDest = 261000
      isFraud = 1
    },
    @{
      step = 2
      type = "CASH_OUT"
      amount = 5000
      nameOrig = "C789"
      oldbalanceOrg = 7000
      newbalanceOrig = 2000
      nameDest = "M111"
      oldbalanceDest = 0
      newbalanceDest = 5000
      isFraud = 0
    }
  )
} | ConvertTo-Json -Depth 5

Invoke-RestMethod -Method Post -Uri "http://localhost:8000/score/batch" -ContentType "application/json" -Body $body
```

## 5. Cau hinh Postman

### `POST /score`

- Method: `POST`
- URL: `http://localhost:8000/score`
- Header: `Content-Type: application/json`
- Body `raw` `JSON`:

```json
{
  "step": 1,
  "type": "TRANSFER",
  "amount": 260000,
  "nameOrig": "C123",
  "oldbalanceOrg": 300000,
  "newbalanceOrig": 100,
  "nameDest": "C456",
  "oldbalanceDest": 1000,
  "newbalanceDest": 261000,
  "isFraud": 1
}
```

### `POST /score/batch`

- Method: `POST`
- URL: `http://localhost:8000/score/batch`
- Header: `Content-Type: application/json`
- Body `raw` `JSON`:

```json
{
  "transactions": [
    {
      "step": 1,
      "type": "TRANSFER",
      "amount": 260000,
      "nameOrig": "C123",
      "oldbalanceOrg": 300000,
      "newbalanceOrig": 100,
      "nameDest": "C456",
      "oldbalanceDest": 1000,
      "newbalanceDest": 261000,
      "isFraud": 1
    },
    {
      "step": 2,
      "type": "CASH_OUT",
      "amount": 5000,
      "nameOrig": "C789",
      "oldbalanceOrg": 7000,
      "newbalanceOrig": 2000,
      "nameDest": "M111",
      "oldbalanceDest": 0,
      "newbalanceDest": 5000,
      "isFraud": 0
    }
  ]
}
```

## 6. Test `GET /health`

```powershell
curl.exe "http://localhost:8000/health"
```

Response mau:

```json
{
  "status": "ok",
  "model_loaded": true,
  "model_version": "v1",
  "model_type": "xgb",
  "prediction_logging_enabled": false
}
```

## 7. Loi thuong gap

### `Connection refused`

Nguyen nhan thuong la API chua chay. Kiem tra lai:

```powershell
uvicorn api.app:app --host 0.0.0.0 --port 8000
```

hoac:

```powershell
docker-compose up -d api
```

### HTTP 422

Nguyen nhan thuong la payload sai field hoac sai kieu du lieu.

Can dung dung cac field sau:

- `step`
- `type`
- `amount`
- `nameOrig`
- `oldbalanceOrg`
- `newbalanceOrig`
- `nameDest`
- `oldbalanceDest`
- `newbalanceDest`
- `isFraud`

### API len nhung `model_loaded = false`

API van co the tra response neu fallback logic con hoat dong, nhung can kiem tra artifact model neu ban ky vong model ML duoc nap day du.
