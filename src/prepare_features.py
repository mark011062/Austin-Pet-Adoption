# src/prepare_features.py

import os
import re
import sys
from dotenv import load_dotenv

from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

SOURCE_SCHEMA = "raw"
SOURCE_TABLE = "raw_aac_intakes_outcomes"
TARGET_SCHEMA = "staging"
TARGET_TABLE = "pet_features"

# Default to false so we do not silently remove valid event history.
DEDUP_ENABLED = os.getenv("DEDUP_ENABLED", "false").strip().lower() in {
    "1", "true", "yes", "y"
}

ALLOWED_ANIMAL_TYPES = {"Dog", "Cat"}


def get_engine():
    conn_str = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(conn_str)


def normalize_column_name(col: str) -> str:
    col = col.strip().lower()
    col = re.sub(r"[^a-z0-9]+", "_", col)
    col = re.sub(r"_+", "_", col)
    return col.strip("_")


def clean_string(val):
    if pd.isna(val):
        return np.nan
    val = str(val).strip()
    if val == "":
        return np.nan
    return val


def clean_string_series(series: pd.Series) -> pd.Series:
    return series.apply(clean_string)


def title_case_safe(val):
    if pd.isna(val):
        return np.nan
    return str(val).strip().title()


def find_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def to_datetime_if_exists(df: pd.DataFrame, col_name: Optional[str]):
    if col_name and col_name in df.columns:
        df[col_name] = pd.to_datetime(df[col_name], errors="coerce")
    return df


def create_age_features(df: pd.DataFrame, age_col: Optional[str]) -> pd.DataFrame:
    """
    Attempts to parse age strings like:
    '2 years', '3 months', '1 week', '5 days'
    into age_in_days and age_bucket.
    """
    if not age_col or age_col not in df.columns:
        df["age_in_days"] = np.nan
        df["age_bucket"] = np.nan
        return df

    def parse_age_to_days(val):
        if pd.isna(val):
            return np.nan

        s = str(val).strip().lower()
        match = re.match(r"(\d+)\s+(\w+)", s)
        if not match:
            return np.nan

        num = int(match.group(1))
        unit = match.group(2)

        if "day" in unit:
            return num
        if "week" in unit:
            return num * 7
        if "month" in unit:
            return num * 30
        if "year" in unit:
            return num * 365

        return np.nan

    df["age_in_days"] = df[age_col].apply(parse_age_to_days)

    def bucket_age(days):
        if pd.isna(days):
            return np.nan
        if days < 180:
            return "baby"
        if days < 365 * 2:
            return "young"
        if days < 365 * 8:
            return "adult"
        return "senior"

    df["age_bucket"] = df["age_in_days"].apply(bucket_age)
    return df


def create_datetime_features(df: pd.DataFrame, dt_col: Optional[str], prefix: str) -> pd.DataFrame:
    if not dt_col or dt_col not in df.columns:
        df[f"{prefix}_year"] = np.nan
        df[f"{prefix}_month"] = np.nan
        df[f"{prefix}_day"] = np.nan
        df[f"{prefix}_dayofweek"] = np.nan
        df[f"{prefix}_is_weekend"] = np.nan
        return df

    df[f"{prefix}_year"] = df[dt_col].dt.year
    df[f"{prefix}_month"] = df[dt_col].dt.month
    df[f"{prefix}_day"] = df[dt_col].dt.day
    df[f"{prefix}_dayofweek"] = df[dt_col].dt.dayofweek
    df[f"{prefix}_is_weekend"] = df[dt_col].dt.dayofweek.isin([5, 6])

    return df


def create_length_of_stay(df: pd.DataFrame, intake_dt_col: Optional[str], outcome_dt_col: Optional[str]) -> pd.DataFrame:
    if intake_dt_col and outcome_dt_col and intake_dt_col in df.columns and outcome_dt_col in df.columns:
        df["length_of_stay_days"] = (df[outcome_dt_col] - df[intake_dt_col]).dt.days
        df.loc[df["length_of_stay_days"] < 0, "length_of_stay_days"] = np.nan
    else:
        df["length_of_stay_days"] = np.nan

    return df


def main():
    engine = get_engine()

    source_fq = f"{SOURCE_SCHEMA}.{SOURCE_TABLE}"
    target_fq = f"{TARGET_SCHEMA}.{TARGET_TABLE}"

    print(f"Reading from {source_fq} ...")
    query = f"SELECT * FROM {source_fq};"
    df = pd.read_sql(query, engine)

    print(f"Loaded {len(df):,} rows")

    print("Original source columns:")
    for col in df.columns:
        print(f" - {col}")

    # Normalize column names
    df.columns = [normalize_column_name(c) for c in df.columns]

    print("Normalized source columns:")
    for col in df.columns:
        print(f" - {col}")

    # Add surrogate row identifier for lineage
    df.insert(0, "source_row_id", range(1, len(df) + 1))

    # Clean all object/string columns
    object_cols = df.select_dtypes(include=["object", "string"]).columns.tolist()
    for col in object_cols:
        df[col] = df[col].apply(clean_string)

    # Try to identify important columns
    animal_id_col = find_col(df, ["animal_id", "animalid"])
    name_col = find_col(df, ["name", "animal_name"])
    animal_type_col = find_col(df, ["animal_type", "animaltype", "type"])
    breed_col = find_col(df, ["breed"])
    color_col = find_col(df, ["color"])
    sex_col = find_col(df, ["sex_upon_intake", "sex_upon_outcome", "sex"])
    age_col = find_col(df, ["age_upon_intake", "age_upon_outcome", "age"])
    intake_dt_col = find_col(df, ["datetime_intake", "intake_datetime", "datetime", "date_time"])
    outcome_dt_col = find_col(df, ["datetime_outcome", "outcome_datetime"])
    intake_type_col = find_col(df, ["intake_type"])
    intake_condition_col = find_col(df, ["intake_condition"])
    outcome_type_col = find_col(df, ["outcome_type"])
    outcome_subtype_col = find_col(df, ["outcome_subtype"])

    print("Detected column mapping:")
    print(f" - source_row_id:      source_row_id")
    print(f" - animal_id:          {animal_id_col}")
    print(f" - name:               {name_col}")
    print(f" - animal_type:        {animal_type_col}")
    print(f" - breed:              {breed_col}")
    print(f" - color:              {color_col}")
    print(f" - sex:                {sex_col}")
    print(f" - age:                {age_col}")
    print(f" - intake_datetime:    {intake_dt_col}")
    print(f" - outcome_datetime:   {outcome_dt_col}")
    print(f" - intake_type:        {intake_type_col}")
    print(f" - intake_condition:   {intake_condition_col}")
    print(f" - outcome_type:       {outcome_type_col}")
    print(f" - outcome_subtype:    {outcome_subtype_col}")

    # Standardize common text columns
    for col in [
        animal_type_col,
        breed_col,
        color_col,
        sex_col,
        intake_type_col,
        intake_condition_col,
        outcome_type_col,
        outcome_subtype_col,
    ]:
        if col and col in df.columns:
            df[col] = df[col].apply(title_case_safe)

    # Restrict project scope to dogs and cats only
    if animal_type_col and animal_type_col in df.columns:
        before_filter = len(df)
        animal_type_counts_before = (
            df[animal_type_col]
            .fillna("NULL")
            .value_counts(dropna=False)
            .to_dict()
        )

        df = df[df[animal_type_col].isin(ALLOWED_ANIMAL_TYPES)].copy()

        after_filter = len(df)
        animal_type_counts_after = (
            df[animal_type_col]
            .fillna("NULL")
            .value_counts(dropna=False)
            .to_dict()
        )

        print(f"Animal scope filter applied: {sorted(ALLOWED_ANIMAL_TYPES)}")
        print(f"Rows before animal filter: {before_filter:,}")
        print(f"Rows after animal filter:  {after_filter:,}")
        print(f"Rows removed by filter:    {before_filter - after_filter:,}")
        print(f"Animal type counts before filter: {animal_type_counts_before}")
        print(f"Animal type counts after filter:  {animal_type_counts_after}")
    else:
        print("WARNING: No animal_type column found. Dog/Cat filter was not applied.")

    # Clean name separately
    if name_col and name_col in df.columns:
        df[name_col] = df[name_col].apply(clean_string)
        df["has_name"] = df[name_col].notna()
    else:
        df["has_name"] = False

    # Datetime parsing
    df = to_datetime_if_exists(df, intake_dt_col)
    df = to_datetime_if_exists(df, outcome_dt_col)

    # Feature engineering
    df = create_age_features(df, age_col)
    df = create_datetime_features(df, intake_dt_col, "intake")
    df = create_datetime_features(df, outcome_dt_col, "outcome")
    df = create_length_of_stay(df, intake_dt_col, outcome_dt_col)

    # Derived flags
    if animal_type_col and animal_type_col in df.columns:
        df["is_dog"] = df[animal_type_col].str.lower().eq("dog")
        df["is_cat"] = df[animal_type_col].str.lower().eq("cat")
    else:
        df["is_dog"] = np.nan
        df["is_cat"] = np.nan

    if outcome_type_col and outcome_type_col in df.columns:
        adopted_values = {"Adoption"}
        transfer_values = {"Transfer"}
        died_values = {"Died", "Euthanasia"}

        df["is_adopted"] = df[outcome_type_col].isin(adopted_values)
        df["is_transferred"] = df[outcome_type_col].isin(transfer_values)
        df["is_died_or_euthanized"] = df[outcome_type_col].isin(died_values)
    else:
        df["is_adopted"] = np.nan
        df["is_transferred"] = np.nan
        df["is_died_or_euthanized"] = np.nan

    # Build a consistent output schema
    feature_df = pd.DataFrame()

    feature_df["source_row_id"] = df["source_row_id"]

    feature_df["animal_id"] = df[animal_id_col] if animal_id_col and animal_id_col in df.columns else np.nan
    feature_df["name"] = df[name_col] if name_col and name_col in df.columns else np.nan
    feature_df["animal_type"] = df[animal_type_col] if animal_type_col and animal_type_col in df.columns else np.nan
    feature_df["breed"] = df[breed_col] if breed_col and breed_col in df.columns else np.nan
    feature_df["color"] = df[color_col] if color_col and color_col in df.columns else np.nan
    feature_df["sex_upon_intake"] = df[sex_col] if sex_col and sex_col in df.columns else np.nan
    feature_df["age_upon_intake"] = df[age_col] if age_col and age_col in df.columns else np.nan
    feature_df["intake_datetime"] = df[intake_dt_col] if intake_dt_col and intake_dt_col in df.columns else np.nan
    feature_df["outcome_datetime"] = df[outcome_dt_col] if outcome_dt_col and outcome_dt_col in df.columns else np.nan
    feature_df["intake_type"] = df[intake_type_col] if intake_type_col and intake_type_col in df.columns else np.nan
    feature_df["intake_condition"] = df[intake_condition_col] if intake_condition_col and intake_condition_col in df.columns else np.nan
    feature_df["outcome_type"] = df[outcome_type_col] if outcome_type_col and outcome_type_col in df.columns else np.nan
    feature_df["outcome_subtype"] = df[outcome_subtype_col] if outcome_subtype_col and outcome_subtype_col in df.columns else np.nan

    feature_df["age_in_days"] = df["age_in_days"]
    feature_df["age_bucket"] = df["age_bucket"]
    feature_df["length_of_stay_days"] = df["length_of_stay_days"]

    feature_df["has_name"] = df["has_name"]
    feature_df["is_dog"] = df["is_dog"]
    feature_df["is_cat"] = df["is_cat"]
    feature_df["is_adopted"] = df["is_adopted"]
    feature_df["is_transferred"] = df["is_transferred"]
    feature_df["is_died_or_euthanized"] = df["is_died_or_euthanized"]

    feature_df["intake_year"] = df["intake_year"]
    feature_df["intake_month"] = df["intake_month"]
    feature_df["intake_day"] = df["intake_day"]
    feature_df["intake_dayofweek"] = df["intake_dayofweek"]
    feature_df["intake_is_weekend"] = df["intake_is_weekend"]

    feature_df["outcome_year"] = df["outcome_year"]
    feature_df["outcome_month"] = df["outcome_month"]
    feature_df["outcome_day"] = df["outcome_day"]
    feature_df["outcome_dayofweek"] = df["outcome_dayofweek"]
    feature_df["outcome_is_weekend"] = df["outcome_is_weekend"]

    # Final cleanup on text columns in the output table
    text_cols = [
        "animal_id",
        "name",
        "animal_type",
        "breed",
        "color",
        "sex_upon_intake",
        "age_upon_intake",
        "intake_type",
        "intake_condition",
        "outcome_type",
        "outcome_subtype",
    ]
    for col in text_cols:
        if col in feature_df.columns:
            feature_df[col] = clean_string_series(feature_df[col])

    # Duplicate diagnostics
    print(f"Dedup enabled: {DEDUP_ENABLED}")

    dedupe_cols = [c for c in feature_df.columns if c != "source_row_id"]
    exact_duplicate_count = int(feature_df.duplicated(subset=dedupe_cols, keep="first").sum())
    print(f"Exact duplicate rows available to remove: {exact_duplicate_count:,}")

    if "animal_id" in feature_df.columns and feature_df["animal_id"].notna().any():
        repeated_animal_id_rows = int(feature_df["animal_id"].duplicated(keep=False).sum())
        unique_animal_ids = feature_df["animal_id"].nunique(dropna=True)
        print(f"Rows sharing the same animal_id: {repeated_animal_id_rows:,}")
        print(f"Unique non-null animal_id values: {unique_animal_ids:,}")
    else:
        print("No usable animal_id column found for animal-level duplicate diagnostics.")

    # Optional dedupe
    before = len(feature_df)
    if DEDUP_ENABLED:
        feature_df = feature_df.drop_duplicates(subset=dedupe_cols).copy()
    after = len(feature_df)

    print(f"Rows before dedupe: {before:,}")
    print(f"Rows after dedupe:  {after:,}")
    print(f"Rows removed:       {before - after:,}")

    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};"))

    print(f"Writing {len(feature_df):,} rows to {target_fq} ...")
    feature_df.to_sql(
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
    for col in feature_df.columns:
        print(f" - {col}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)