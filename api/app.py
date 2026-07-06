from fastapi import FastAPI

from .schemas import BatchScoreRequest, BatchScoreResponse, HealthResponse, ScoreRequest, ScoreResponse
from .service import health_payload, score_transaction


app = FastAPI(
    title="Fraud Scoring API",
    version="1.0.0",
    description="Real-time transaction scoring API for the fraud detection pipeline.",
)


@app.get("/health", response_model=HealthResponse)
def healthcheck() -> HealthResponse:
    return HealthResponse(**health_payload())


@app.post("/score", response_model=ScoreResponse)
def score(payload: ScoreRequest) -> ScoreResponse:
    return score_transaction(payload)


@app.post("/score/batch", response_model=BatchScoreResponse)
def score_batch(payload: BatchScoreRequest) -> BatchScoreResponse:
    return BatchScoreResponse(predictions=[score_transaction(item) for item in payload.transactions])
