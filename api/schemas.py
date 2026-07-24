from datetime import datetime
from typing import Annotated, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class ScoreRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    event_id: Optional[str] = Field(default=None, description="Optional transaction identifier")
    event_time: Optional[datetime] = Field(default=None, description="Optional event timestamp in ISO-8601")
    producer_ts: Optional[datetime] = Field(default=None, description="Optional producer timestamp in ISO-8601")
    step: int = Field(..., ge=0)
    txn_type: Annotated[str, Field(alias="type", min_length=1)]
    amount: float = Field(..., ge=0)
    name_orig: Annotated[str, Field(alias="nameOrig", min_length=1)]
    oldbalance_org: Annotated[float, Field(alias="oldbalanceOrg")]
    newbalance_orig: Annotated[float, Field(alias="newbalanceOrig")]
    name_dest: Annotated[str, Field(alias="nameDest", min_length=1)]
    oldbalance_dest: Annotated[float, Field(alias="oldbalanceDest")]
    newbalance_dest: Annotated[float, Field(alias="newbalanceDest")]
    is_fraud: Annotated[int, Field(alias="isFraud", ge=0, le=1)] = 0
    schema_version: int = Field(default=1, ge=1)


class ScoreResponse(BaseModel):
    event_id: str
    is_alert: bool
    risk_score: float
    severity: str
    ml_score: float
    ml_model_version: str
    triggered_rules: List[str]


class HealthResponse(BaseModel):
    status: str
    model_loaded: bool
    model_version: str
    model_type: str
    prediction_logging_enabled: bool


class BatchScoreRequest(BaseModel):
    transactions: List[ScoreRequest] = Field(..., min_length=1)


class BatchScoreResponse(BaseModel):
    predictions: List[ScoreResponse]
