FROM python:3.11-slim

WORKDIR /app

COPY api/requirements.txt /tmp/requirements.txt
RUN python -m pip install --no-cache-dir -r /tmp/requirements.txt

COPY fraud_pipeline /app/fraud_pipeline
COPY model /app/model
COPY api /app/api

CMD ["uvicorn", "api.app:app", "--host", "0.0.0.0", "--port", "8000"]
