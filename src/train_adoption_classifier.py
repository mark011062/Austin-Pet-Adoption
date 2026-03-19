import os
import sys
from pathlib import Path

import joblib
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
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
MODEL_PATH = MODEL_DIR / "adoption_30day_classifier.joblib"


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

    target_col = "target_adopted_within_30_days"

    df = df[df[target_col].notna()].copy()

    y = df[target_col]

    drop_cols = [
        "source_row_id",
        "age_upon_intake",
        "breed",
        "target_days_to_adoption",
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

    categorical_transformer = Pipeline([
        ("imputer", SimpleImputer(strategy="constant", fill_value="Unknown")),
        ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
    ])

    numeric_transformer = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
    ])

    preprocessor = ColumnTransformer([
        ("cat", categorical_transformer, categorical_features),
        ("num", numeric_transformer, numeric_features),
    ])

    model = HistGradientBoostingClassifier(
        max_iter=300,
        learning_rate=0.05,
        max_depth=6,
        random_state=42,
    )

    pipeline = Pipeline([
        ("preprocessor", preprocessor),
        ("model", model),
    ])

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
        stratify=y,
    )

    print(f"Training rows: {len(X_train):,}")
    print(f"Test rows:     {len(X_test):,}")

    pipeline.fit(X_train, y_train)

    preds = pipeline.predict(X_test)

    print("\nModel Evaluation")
    print(f"Accuracy:  {accuracy_score(y_test, preds):.4f}")
    print(f"Precision: {precision_score(y_test, preds):.4f}")
    print(f"Recall:    {recall_score(y_test, preds):.4f}")
    print(f"F1 Score:  {f1_score(y_test, preds):.4f}")

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipeline, MODEL_PATH)

    print(f"\nSaved model to: {MODEL_PATH}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)