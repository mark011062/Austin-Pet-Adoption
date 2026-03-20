import os
from datetime import datetime

import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

API_URL = "http://127.0.0.1:8000/predictions/adoption-within-days"

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "55432")
DB_NAME = os.getenv("DB_NAME", "pet_adoption_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def get_engine():
    conn_str = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    return create_engine(conn_str)


@st.cache_data
def load_dropdown_data():
    engine = get_engine()

    breed_query = text("""
        SELECT DISTINCT animal_type, breed_group
        FROM ml.adoption_training_data
        WHERE animal_type IN ('Dog', 'Cat')
          AND breed_group IS NOT NULL
          AND TRIM(breed_group) <> ''
        ORDER BY animal_type, breed_group;
    """)

    color_query = text("""
        SELECT DISTINCT animal_type, color
        FROM ml.adoption_training_data
        WHERE animal_type IN ('Dog', 'Cat')
          AND color IS NOT NULL
          AND TRIM(color) <> ''
        ORDER BY animal_type, color;
    """)

    intake_type_query = text("""
        SELECT DISTINCT intake_type
        FROM ml.adoption_training_data
        WHERE intake_type IS NOT NULL
          AND TRIM(intake_type) <> ''
        ORDER BY intake_type;
    """)

    intake_condition_query = text("""
        SELECT DISTINCT intake_condition
        FROM ml.adoption_training_data
        WHERE intake_condition IS NOT NULL
          AND TRIM(intake_condition) <> ''
        ORDER BY intake_condition;
    """)

    breed_df = pd.read_sql(breed_query, engine)
    color_df = pd.read_sql(color_query, engine)
    intake_type_df = pd.read_sql(intake_type_query, engine)
    intake_condition_df = pd.read_sql(intake_condition_query, engine)

    return breed_df, color_df, intake_type_df, intake_condition_df


def get_animal_options(df: pd.DataFrame, animal_type: str, value_col: str) -> list[str]:
    options = (
        df.loc[df["animal_type"] == animal_type, value_col]
        .dropna()
        .astype(str)
        .sort_values()
        .unique()
        .tolist()
    )
    return options


def get_single_column_options(df: pd.DataFrame, value_col: str) -> list[str]:
    options = (
        df[value_col]
        .dropna()
        .astype(str)
        .sort_values()
        .unique()
        .tolist()
    )
    return options


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


st.set_page_config(page_title="Pet Adoption Predictor", layout="centered")

st.title("🐾 Pet Adoption Predictor V3")
st.write("Enter intake details to predict the likelihood that a dog or cat will be adopted within a selected number of days.")

breed_df, color_df, intake_type_df, intake_condition_df = load_dropdown_data()

animal_type = st.selectbox("Animal Type", ["Dog", "Cat"])

breed_options = get_animal_options(breed_df, animal_type, "breed_group")
color_options = get_animal_options(color_df, animal_type, "color")
intake_type_options = get_single_column_options(intake_type_df, "intake_type")
intake_condition_options = get_single_column_options(intake_condition_df, "intake_condition")

if not breed_options:
    breed_options = ["Unknown"]

if not color_options:
    color_options = ["Unknown"]

if not intake_type_options:
    intake_type_options = ["Unknown"]

if not intake_condition_options:
    intake_condition_options = ["Unknown"]

if animal_type == "Dog" and "Labrador" in breed_options:
    breed_index = breed_options.index("Labrador")
elif animal_type == "Cat" and "Domestic" in breed_options:
    breed_index = breed_options.index("Domestic")
else:
    breed_index = 0

breed_group = st.selectbox("Breed Group", breed_options, index=breed_index)
color = st.selectbox("Color", color_options, index=0)

sex_upon_intake = st.selectbox(
    "Sex Upon Intake",
    ["Neutered Male", "Spayed Female", "Intact Male", "Intact Female"]
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
name_length = has_name

intake_type = st.selectbox("Intake Type", intake_type_options, index=0)
intake_condition = st.selectbox("Condition at Intake", intake_condition_options, index=0)

intake_date = st.date_input("Intake Date", datetime(2017, 6, 15))

days_window = st.selectbox(
    "Prediction Window (Days)",
    [7, 14, 30, 60],
    index=2
)

is_mix = 1 if "mix" in breed_group.lower() else 0

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
        "color": color,
        "sex_upon_intake": sex_upon_intake,
        "age_bucket": age_bucket,
        "age_in_days": age_in_days,
        "has_name": has_name,
        "name_length": name_length,
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
        response = requests.post(API_URL, json=payload, timeout=30)
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

        st.metric(f"Confidence for {returned_window}-day window", f"{prob:.2%}")

        with st.expander("Show model inputs used"):
            st.json(payload)

        with st.expander("Show API response"):
            st.json(result)

    except Exception as e:
        st.error(f"Error calling prediction API: {e}")