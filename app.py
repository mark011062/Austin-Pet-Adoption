import re
from datetime import datetime

import requests
import streamlit as st

API_BASE = "https://austin-pet-adoption.onrender.com"
PREDICTION_API_URL = f"{API_BASE}/predictions/adoption-within-days"
DROPDOWN_API_URL = f"{API_BASE}/analytics/dropdowns"


@st.cache_data(ttl=3600)
def load_dropdown_data():
    try:
        response = requests.get(DROPDOWN_API_URL, timeout=30)
        response.raise_for_status()
        data = response.json()

        return {
            "animal_types": data.get("animal_types", ["Dog", "Cat"]),
            "breeds": data.get("breeds", {}),
            "colors": data.get("colors", {}),
            "intake_types": data.get("intake_types", []),
            "intake_conditions": data.get("intake_conditions", []),
        }
    except Exception:
        return {
            "animal_types": ["Dog", "Cat"],
            "breeds": {
                "Dog": [
                    "Labrador Retriever",
                    "German Shepherd",
                    "Pit Bull Mix",
                    "Chihuahua Mix",
                ],
                "Cat": [
                    "Domestic Shorthair",
                    "Domestic Longhair",
                    "Siamese",
                    "Cat Mix",
                ],
            },
            "colors": {
                "Dog": ["Black", "Brown", "White", "Tan"],
                "Cat": ["Black", "Gray", "White", "Orange"],
            },
            "intake_types": [
                "Stray",
                "Owner Surrender",
                "Public Assist",
                "Wildlife",
            ],
            "intake_conditions": [
                "Normal",
                "Injured",
                "Sick",
                "Aged",
                "Nursing",
            ],
        }


def convert_age_to_days(age_value: int, age_unit: str) -> int:
    if age_unit == "days":
        return age_value
    if age_unit == "weeks":
        return age_value * 7
    if age_unit == "months":
        return age_value * 30
    if age_unit == "years":
        return age_value * 365
    return age_value


def bucket_age(age_in_days: int) -> str:
    if age_in_days < 180:
        return "baby"
    if age_in_days < 365 * 2:
        return "young"
    if age_in_days < 365 * 8:
        return "adult"
    return "senior"


def simplify_breed_for_display(breed_clean: str) -> str:
    if not breed_clean:
        return "Unknown"

    breed = str(breed_clean).strip()
    if not breed:
        return "Unknown"

    breed_lower = breed.lower()

    if breed_lower == "unknown":
        return "Unknown"

    if "/" in breed:
        first_part = breed.split("/")[0].strip()
        if "mix" not in first_part.lower():
            return f"{first_part} Mix"
        return first_part

    if re.search(r"\bmix\b", breed_lower):
        return breed

    return breed


def build_friendly_breed_options(breeds: list[str]) -> list[str]:
    friendly = sorted(
        {simplify_breed_for_display(b) for b in breeds if str(b).strip()}
    )
    return friendly


def map_breed_to_group(selected_breed: str, animal_type: str) -> str:
    breed = selected_breed.lower().strip()

    if animal_type.lower() == "cat":
        return "Cat"

    if "pit bull" in breed or "staffordshire" in breed or "bulldog" in breed or "boxer" in breed:
        return "Pit Bull / Bully"

    if "labrador" in breed or "retriever" in breed:
        return "Labrador / Retriever"

    if "german shepherd" in breed:
        return "German Shepherd"

    if (
        "chihuahua" in breed
        or "pomeranian" in breed
        or "miniature poodle" in breed
        or "miniature schnauzer" in breed
        or "lhasa apso" in breed
        or "maltese" in breed
        or "shih tzu" in breed
        or "yorkshire" in breed
        or "papillon" in breed
        or "pekingese" in breed
        or "pug" in breed
        or "dachshund" in breed
        or "min pin" in breed
        or "miniature pinscher" in breed
    ):
        return "Small Breed"

    if (
        "beagle" in breed
        or "hound" in breed
        or "coonhound" in breed
        or "plott" in breed
        or "whippet" in breed
        or "greyhound" in breed
        or "rhodesian ridgeback" in breed
        or "pharaoh hound" in breed
        or "harrier" in breed
        or "black mouth cur" in breed
    ):
        return "Hound"

    if (
        "collie" in breed
        or "australian shepherd" in breed
        or "australian cattle dog" in breed
        or "australian kelpie" in breed
        or "corgi" in breed
        or "heeler" in breed
        or "belgian malinois" in breed
        or "shetland sheepdog" in breed
    ):
        return "Herding Breed"

    if (
        "rottweiler" in breed
        or "doberman" in breed
        or "mastiff" in breed
        or "great dane" in breed
        or "great pyrenees" in breed
        or "saint bernard" in breed
        or "bernese mountain dog" in breed
        or "newfoundland" in breed
        or "akita" in breed
        or "husky" in breed
        or "alaskan malamute" in breed
        or "cane corso" in breed
        or "chow chow" in breed
    ):
        return "Working Breed"

    if (
        "jack russell" in breed
        or "rat terrier" in breed
        or "fox terrier" in breed
        or "airedale" in breed
        or "west highland" in breed
        or "scottish terrier" in breed
        or "cairn terrier" in breed
        or "terrier" in breed
    ):
        return "Terrier (non-bully)"

    if "toy poodle" in breed or "poodle toy" in breed:
        return "Toy Breed"

    if (
        "pointer" in breed
        or "spaniel" in breed
        or "setter" in breed
        or "vizsla" in breed
        or "weimaraner" in breed
    ):
        return "Sporting Breed"

    if (
        "catahoula" in breed
        or "carolina dog" in breed
        or "blue lacy" in breed
        or "shiba inu" in breed
    ):
        return "Other Regional / Primitive"

    if "mix" in breed:
        return "Mixed Breed (General)"

    return "Other"


def default_breed_index(breed_options: list[str], animal_type: str) -> int:
    if not breed_options:
        return 0

    if animal_type == "Dog":
        preferred = [
            "Labrador Retriever",
            "German Shepherd",
            "Chihuahua Mix",
            "Pit Bull Mix",
        ]
    else:
        preferred = [
            "Domestic Shorthair",
            "Domestic Longhair",
            "Siamese",
        ]

    for option in preferred:
        if option in breed_options:
            return breed_options.index(option)

    return 0


st.set_page_config(page_title="Pet Adoption Predictor", layout="centered")

st.title("🐾 Pet Adoption Predictor")
st.write(
    "Enter intake details to predict the likelihood that a dog or cat "
    "will be adopted within a selected number of days."
)

dropdown_data = load_dropdown_data()

animal_types = dropdown_data.get("animal_types", ["Dog", "Cat"])
breeds_by_type = dropdown_data.get("breeds", {})
colors_by_type = dropdown_data.get("colors", {})
intake_type_options = dropdown_data.get("intake_types", [])
intake_condition_options = dropdown_data.get("intake_conditions", [])

animal_type = st.selectbox("Animal Type", animal_types)

breed_options = build_friendly_breed_options(breeds_by_type.get(animal_type, []))
color_options = colors_by_type.get(animal_type, [])

if not breed_options:
    breed_options = ["Unknown"]

if not color_options:
    color_options = ["Unknown"]

if not intake_type_options:
    intake_type_options = ["Unknown"]

if not intake_condition_options:
    intake_condition_options = ["Unknown"]

selected_breed = st.selectbox(
    "Breed",
    breed_options,
    index=default_breed_index(breed_options, animal_type),
)

color_primary = st.selectbox("Color", color_options, index=0)

sex_upon_intake = st.selectbox(
    "Sex Upon Intake",
    ["Neutered Male", "Spayed Female", "Intact Male", "Intact Female"],
)

st.subheader("Age")

age_col1, age_col2 = st.columns(2)

with age_col1:
    age_value = st.number_input("Age Value", min_value=0, value=1, step=1)

with age_col2:
    age_unit = st.selectbox("Age Unit", ["years", "months", "weeks", "days"])

age_in_days = convert_age_to_days(age_value, age_unit)
age_bucket = bucket_age(age_in_days)
is_puppy_kitten = 1 if age_bucket == "baby" else 0
is_senior = 1 if age_bucket == "senior" else 0

st.caption(f"Calculated age: {age_in_days} days | Age bucket: {age_bucket}")

has_name_label = st.selectbox("Does the animal have a name?", ["Yes", "No"])
has_name = 1 if has_name_label == "Yes" else 0

intake_type = st.selectbox("Intake Type", intake_type_options, index=0)
intake_condition = st.selectbox("Condition at Intake", intake_condition_options, index=0)

intake_date = st.date_input("Intake Date", datetime(2017, 6, 15))

days_window = st.selectbox(
    "Prediction Window (Days)",
    [7, 14, 30, 60],
    index=2,
)

is_mix = 1 if "mix" in selected_breed.lower() else 0
breed_group = map_breed_to_group(selected_breed, animal_type)

intake_year = intake_date.year
intake_month = intake_date.month
intake_day = intake_date.day
intake_dayofweek = intake_date.weekday()
intake_is_weekend = 1 if intake_dayofweek in [5, 6] else 0
is_summer = 1 if intake_month in [6, 7, 8] else 0
is_holiday_season = 1 if intake_month in [11, 12] else 0

if st.button("Predict Adoption"):
    payload = {
        "animal_type": animal_type,
        "breed_group": breed_group,
        "is_mix": is_mix,
        "color_primary": color_primary,
        "sex_upon_intake": sex_upon_intake,
        "age_bucket": age_bucket,
        "age_in_days": age_in_days,
        "has_name": has_name,
        "is_puppy_kitten": is_puppy_kitten,
        "is_senior": is_senior,
        "intake_type": intake_type,
        "intake_condition": intake_condition,
        "intake_year": intake_year,
        "intake_month": intake_month,
        "intake_day": intake_day,
        "intake_dayofweek": intake_dayofweek,
        "intake_is_weekend": intake_is_weekend,
        "is_summer": is_summer,
        "is_holiday_season": is_holiday_season,
        "days_window": days_window,
    }

    try:
        response = requests.post(PREDICTION_API_URL, json=payload, timeout=30)
        response.raise_for_status()
        result = response.json()

        pred = result["prediction_adopted_within_window"]
        prob = result["probability_adopted_within_window"]
        returned_window = result["days_window"]

        st.subheader("Prediction Result")

        if pred == 1:
            st.success(f"Likely to be adopted within {returned_window} days")
        else:
            st.warning(f"Less likely to be adopted within {returned_window} days")

        st.metric(
            f"Confidence for {returned_window}-day window",
            f"{prob:.2%}",
        )

        with st.expander("Show model inputs used"):
            st.json(
                {
                    "selected_breed": selected_breed,
                    "derived_breed_group": breed_group,
                    **payload,
                }
            )

        with st.expander("Show API response"):
            st.json(result)

    except requests.exceptions.HTTPError as e:
        if e.response is not None:
            st.error(f"Error calling prediction API: {e.response.text}")
        else:
            st.error(f"Error calling prediction API: {e}")

    except Exception as e:
        st.error(f"Error calling prediction API: {e}")