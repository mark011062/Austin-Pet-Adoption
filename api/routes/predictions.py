import os
from pathlib import Path

import joblib
import pandas as pd
from dotenv import load_dotenv
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

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "55432")
DB_NAME = os.getenv("DB_NAME", "pet_adoption_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")


@router.get("/health")
def prediction_health():
    return {
        "status": "ok",
        "available_models": {k: str(v) for k, v in MODEL_PATHS.items()},
    }


@router.post("/adoption-within-days")
def predict_adoption_within_days(payload: AdoptionPredictionRequest):
    payload_dict = payload.model_dump()
    days_window = payload_dict.pop("days_window")

    model_path = MODEL_PATHS[days_window]
    model = joblib.load(model_path)

    input_data = {
        "animal_type": payload_dict["animal_type"],
        "breed_group": payload_dict["breed_group"],
        "is_mix": payload_dict["is_mix"],
        "color_primary": payload_dict["color_primary"],
        "sex_upon_intake": payload_dict["sex_upon_intake"],
        "age_bucket": payload_dict["age_bucket"],
        "age_in_days": payload_dict["age_in_days"],
        "has_name": payload_dict["has_name"],
        "intake_type": payload_dict["intake_type"],
        "intake_condition": payload_dict["intake_condition"],
        "intake_year": payload_dict["intake_year"],
        "intake_month": payload_dict["intake_month"],
        "intake_day": payload_dict["intake_day"],
        "intake_dayofweek": payload_dict["intake_dayofweek"],
        "intake_is_weekend": payload_dict["intake_is_weekend"],
        "is_puppy_kitten": payload_dict["is_puppy_kitten"],
        "is_senior": payload_dict["is_senior"],
        "is_summer": payload_dict["is_summer"],
        "is_holiday_season": payload_dict["is_holiday_season"],
    }

    input_df = pd.DataFrame([input_data])

    prediction = int(model.predict(input_df)[0])
    probability = round(float(model.predict_proba(input_df)[0][1]), 4)

    return {
        "days_window": days_window,
        "prediction_adopted_within_window": prediction,
        "probability_adopted_within_window": probability,
    }