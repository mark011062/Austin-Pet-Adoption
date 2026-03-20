from pydantic import BaseModel
from typing import Literal


class AdoptionPredictionRequest(BaseModel):
    animal_type: str
    breed_group: str
    is_mix: int
    color: str
    sex_upon_intake: str
    age_bucket: str
    age_in_days: float
    has_name: int
    name_length: int
    is_puppy_kitten: int
    is_senior: int
    intake_type: str
    intake_condition: str
    intake_year: int
    intake_month: int
    intake_day: int
    intake_dayofweek: int
    intake_is_weekend: int
    is_summer: int
    is_holiday_season: int

    days_window: Literal[7, 14, 30, 60]