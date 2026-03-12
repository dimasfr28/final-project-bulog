from pydantic import BaseModel
from datetime import datetime

class LoginRequest(BaseModel):
    username: str
    password: str

class LoginResponse(BaseModel):
    access_token: str
    token_type: str
    username: str

class DashboardStats(BaseModel):
    total_records: int
    last_updated: datetime

class PredictiveData(BaseModel):
    metric: str
    value: float
    prediction: float
