from pathlib import Path

import joblib
import pandas as pd
from fastapi import APIRouter

from api.schemas import AdoptionPredictionRequest

router = APIRouter()

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MODEL_PATHS = {
    7: PROJECT_ROOT / "models" / "adoption_7day_classifier.joblib",
    14: PROJECT_ROOT / "models" / "adoption_14day_classifier.joblib",
    30: PROJECT_ROOT / "models" / "adoption_30day_classifier.joblib",
    60: PROJECT_ROOT / "models" / "adoption_60day_classifier.joblib",
}





@router.get("/health")
def prediction_health():
    return {
        "status": "ok",
        "model_path": str(MODEL_PATH),
    }


@router.post("/adoption-within-days")
def predict_adoption_within_days(payload: AdoptionPredictionRequest):
    payload_dict = payload.model_dump()
    days_window = payload_dict.pop("days_window")
    model_path = MODEL_PATHS[days_window]
    model = joblib.load(model_path)

    input_df = pd.DataFrame([payload_dict])

    prediction = model.predict(input_df)[0]
    probability = model.predict_proba(input_df)[0][1]

    return {
        "days_window": days_window,
        "prediction_adopted_within_window": int(prediction),
        "probability_adopted_within_window": round(float(probability), 4),
    }