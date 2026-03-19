import os
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "55432")
DB_NAME = os.getenv("DB_NAME", "pet_adoption_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")

SOURCE_SCHEMA = "ml"
SOURCE_TABLE = "adoption_training_data"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODEL_DIR = PROJECT_ROOT / "models"
MODEL_PATH = MODEL_DIR / "adoption_days_model.joblib"


def get_engine():
    conn_str = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    return create_engine(conn_str)


def main():
    engine = get_engine()
    source_fq = f"{SOURCE_SCHEMA}.{SOURCE_TABLE}"

    print(f"Reading training data from {source_fq} ...")
    df = pd.read_sql(f"SELECT * FROM {source_fq};", engine)

    print(f"Loaded {len(df):,} rows")

    if df.empty:
        raise ValueError("Training dataset is empty.")

    target_col = "target_days_to_adoption"

    y_raw = pd.to_numeric(df[target_col], errors="coerce")
    valid_mask = y_raw.notna() & (y_raw >= 0)
    df = df.loc[valid_mask].copy()
    y_raw = y_raw.loc[valid_mask].copy()

    if df.empty:
        raise ValueError("No valid target rows found after filtering.")

    y = np.log1p(y_raw)

    drop_cols = [
        "source_row_id",
        "age_upon_intake",
        "breed",
        target_col,
    ]

    X = df.drop(columns=drop_cols, errors="ignore")

    categorical_features = [
        "animal_type",
        "breed_group",
        "color",
        "sex_upon_intake",
        "age_bucket",
        "intake_type",
        "intake_condition",
    ]

    numeric_features = [
        "age_in_days",
        "has_name",
        "name_length",
        "is_mix",
        "is_puppy_kitten",
        "is_senior",
        "intake_year",
        "intake_month",
        "intake_day",
        "intake_dayofweek",
        "intake_is_weekend",
        "is_summer",
        "is_holiday_season",
    ]

    categorical_features = [c for c in categorical_features if c in X.columns]
    numeric_features = [c for c in numeric_features if c in X.columns]

    categorical_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="constant", fill_value="Unknown")),
            ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
        ]
    )

    numeric_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
        ]
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("cat", categorical_transformer, categorical_features),
            ("num", numeric_transformer, numeric_features),
        ],
        remainder="drop",
    )

    model = HistGradientBoostingRegressor(
        max_iter=300,
        learning_rate=0.05,
        max_depth=6,
        min_samples_leaf=20,
        random_state=42,
    )

    pipeline = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("model", model),
        ]
    )

    X_train, X_test, y_train, y_test_log, _, y_test_raw = train_test_split(
        X,
        y,
        y_raw,
        test_size=0.2,
        random_state=42,
    )

    print(f"Training rows: {len(X_train):,}")
    print(f"Test rows:     {len(X_test):,}")

    pipeline.fit(X_train, y_train)

    preds_log = pipeline.predict(X_test)
    preds = np.expm1(preds_log)
    preds = np.clip(preds, 0, None)

    mae = mean_absolute_error(y_test_raw, preds)
    rmse = mean_squared_error(y_test_raw, preds) ** 0.5
    r2 = r2_score(y_test_raw, preds)

    print("\nModel Evaluation")
    print(f"MAE:  {mae:.2f}")
    print(f"RMSE: {rmse:.2f}")
    print(f"R2:   {r2:.4f}")

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipeline, MODEL_PATH)

    print(f"\nSaved model to: {MODEL_PATH}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)