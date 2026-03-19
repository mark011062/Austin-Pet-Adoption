from pathlib import Path

import joblib
import pandas as pd
from fastapi import APIRouter

from api.schemas import AdoptionPredictionRequest

router = APIRouter()

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MODEL_PATH = PROJECT_ROOT / "models" / "adoption_30day_classifier.joblib"

model = joblib.load(MODEL_PATH)


@router.get("/health")
def prediction_health():
    return {
        "status": "ok",
        "model_path": str(MODEL_PATH),
    }


@router.post("/adoption-within-30-days")
def predict_adoption_within_30_days(payload: AdoptionPredictionRequest):
    input_df = pd.DataFrame([payload.model_dump()])

    prediction = model.predict(input_df)[0]
    probability = model.predict_proba(input_df)[0][1]

    return {
        "prediction_adopted_within_30_days": int(prediction),
        "probability_adopted_within_30_days": round(float(probability), 4),
    }