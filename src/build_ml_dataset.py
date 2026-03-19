import os
import sys
from dotenv import load_dotenv

import pandas as pd
from sqlalchemy import create_engine, text

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "55432")
DB_NAME = os.getenv("DB_NAME", "pet_adoption_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")

SOURCE_SCHEMA = "staging"
SOURCE_TABLE = "pet_features"
TARGET_SCHEMA = "ml"
TARGET_TABLE = "adoption_training_data"


def get_engine():
    conn_str = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    return create_engine(conn_str)


def build_breed_group(val):
    if pd.isna(val):
        return "Unknown"

    s = str(val).strip()
    if not s:
        return "Unknown"

    s = s.replace("/", " ")
    parts = s.split()
    if not parts:
        return "Unknown"

    return parts[0].title()


def main():
    engine = get_engine()
    source_fq = f"{SOURCE_SCHEMA}.{SOURCE_TABLE}"
    target_fq = f"{TARGET_SCHEMA}.{TARGET_TABLE}"

    query = f"""
        SELECT
            source_row_id,
            animal_type,
            breed,
            color,
            sex_upon_intake,
            age_upon_intake,
            age_in_days,
            age_bucket,
            has_name,
            intake_type,
            intake_condition,
            intake_year,
            intake_month,
            intake_day,
            intake_dayofweek,
            intake_is_weekend,
            length_of_stay_days AS target_days_to_adoption
        FROM {source_fq}
        WHERE animal_type IN ('Dog', 'Cat')
          AND outcome_type = 'Adoption'
          AND length_of_stay_days IS NOT NULL
          AND length_of_stay_days >= 0;
    """

    print(f"Reading ML source data from {source_fq} ...")
    df = pd.read_sql(query, engine)

    print(f"Loaded {len(df):,} training rows")

    if df.empty:
        raise ValueError("No rows found for ML training dataset.")

    categorical_fill_cols = [
        "animal_type",
        "breed",
        "color",
        "sex_upon_intake",
        "age_bucket",
        "intake_type",
        "intake_condition",
    ]
    for col in categorical_fill_cols:
        if col in df.columns:
            df[col] = df[col].fillna("Unknown")

    if "age_in_days" in df.columns:
        df["age_in_days"] = pd.to_numeric(df["age_in_days"], errors="coerce")

    if "has_name" in df.columns:
        df["has_name"] = df["has_name"].fillna(False).astype(int)

    if "intake_is_weekend" in df.columns:
        df["intake_is_weekend"] = df["intake_is_weekend"].fillna(False).astype(int)

    df["is_mix"] = df["breed"].str.contains("Mix", case=False, na=False).astype(int)
    df["breed_group"] = df["breed"].apply(build_breed_group)

    df["name_length"] = df["has_name"].astype(int)

    df["is_puppy_kitten"] = (
        df["age_in_days"].fillna(999999) < 180
    ).astype(int)

    df["is_senior"] = (
        df["age_in_days"].fillna(-1) >= (365 * 8)
    ).astype(int)

    df["is_summer"] = df["intake_month"].isin([6, 7, 8]).astype(int)
    df["is_holiday_season"] = df["intake_month"].isin([11, 12]).astype(int)

    df["target_adopted_within_30_days"] = (
        df["target_days_to_adoption"] <= 30
    ).astype(int)

    ordered_cols = [
        "source_row_id",
        "animal_type",
        "breed",
        "breed_group",
        "is_mix",
        "color",
        "sex_upon_intake",
        "age_upon_intake",
        "age_in_days",
        "age_bucket",
        "has_name",
        "name_length",
        "is_puppy_kitten",
        "is_senior",
        "intake_type",
        "intake_condition",
        "intake_year",
        "intake_month",
        "intake_day",
        "intake_dayofweek",
        "intake_is_weekend",
        "is_summer",
        "is_holiday_season",
        "target_days_to_adoption",
        "target_adopted_within_30_days",
    ]
    df = df[[c for c in ordered_cols if c in df.columns]]

    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};"))

    print(f"Writing {len(df):,} rows to {target_fq} ...")
    df.to_sql(
        TARGET_TABLE,
        engine,
        schema=TARGET_SCHEMA,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=5000,
    )

    print("Done.")
    print(f"Created table: {target_fq}")
    print("Columns:")
    for col in df.columns:
        print(f" - {col}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)