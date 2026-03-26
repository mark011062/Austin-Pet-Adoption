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
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
)
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
RESULTS_DIR = PROJECT_ROOT / "results"


def get_engine():
    conn_str = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    return create_engine(conn_str)


def main():
    engine = get_engine()
    source_fq = f"{SOURCE_SCHEMA}.{SOURCE_TABLE}"
    adoption_windows = [7, 14, 30, 60]

    print(f"Reading training data from {source_fq} ...")
    df = pd.read_sql(f"SELECT * FROM {source_fq};", engine)

    print(f"Loaded {len(df):,} rows")

    if df.empty:
        raise ValueError("Training table is empty.")

    print("\nChecking available target columns...")
    target_cols = []
    for window in adoption_windows:
        col = f"target_adopted_within_{window}_days"
        if col in df.columns:
            print(f"FOUND:   {col}")
            target_cols.append(col)
        else:
            print(f"MISSING: {col}")

    if not target_cols:
        raise ValueError("No target columns found in training table.")

    # Columns that should never be used as model features
    base_drop_cols = [
        "source_row_id",
        "age_upon_intake",
        "breed",
        "breed_clean",
        "color",
        "target_days_to_adoption",
    ]

    # Drop all target columns from X to prevent leakage
    all_drop_cols = base_drop_cols + target_cols

    X = df.drop(columns=all_drop_cols, errors="ignore")

    categorical_features = [
        "animal_type",
        "breed_group",
        "color_primary",
        "sex_upon_intake",
        "age_bucket",
        "intake_type",
        "intake_condition",
    ]

    numeric_features = [
        "age_in_days",
        "has_name",
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

    print("\nCategorical features used:")
    for col in categorical_features:
        print(f" - {col}")

    print("\nNumeric features used:")
    for col in numeric_features:
        print(f" - {col}")

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
        ]
    )

    results = []
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    for window in adoption_windows:
        target_col = f"target_adopted_within_{window}_days"
        if target_col not in df.columns:
            continue

        y = df[target_col]

        print(f"\n{'=' * 60}")
        print(f"Training model for adoption within {window} days")
        print(f"{'=' * 60}")

        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=0.2,
            random_state=42,
            stratify=y,
        )

        print(f"Training rows: {len(X_train):,}")
        print(f"Test rows:     {len(X_test):,}")

        model = HistGradientBoostingClassifier(
            max_iter=300,
            learning_rate=0.05,
            max_depth=6,
            random_state=42,
        )

        pipeline = Pipeline(
            steps=[
                ("preprocessor", preprocessor),
                ("model", model),
            ]
        )

        pipeline.fit(X_train, y_train)

        preds = pipeline.predict(X_test)
        probs = pipeline.predict_proba(X_test)[:, 1]

        accuracy = accuracy_score(y_test, preds)
        precision = precision_score(y_test, preds, zero_division=0)
        recall = recall_score(y_test, preds, zero_division=0)
        f1 = f1_score(y_test, preds, zero_division=0)
        roc_auc = roc_auc_score(y_test, probs)

        print("\nModel Evaluation")
        print(f"Accuracy:  {accuracy:.4f}")
        print(f"Precision: {precision:.4f}")
        print(f"Recall:    {recall:.4f}")
        print(f"F1 Score:  {f1:.4f}")
        print(f"ROC-AUC:   {roc_auc:.4f}")

        model_path = MODEL_DIR / f"adoption_{window}day_classifier.joblib"
        joblib.dump(pipeline, model_path)
        print(f"\nSaved model to: {model_path}")

        results.append(
            {
                "target": target_col,
                "window_days": window,
                "accuracy": round(accuracy, 4),
                "precision": round(precision, 4),
                "recall": round(recall, 4),
                "f1_score": round(f1, 4),
                "roc_auc": round(roc_auc, 4),
                "train_rows": len(X_train),
                "test_rows": len(X_test),
                "model_path": str(model_path),
            }
        )

    if results:
        results_df = pd.DataFrame(results)
        results_path = RESULTS_DIR / "adoption_classifier_results.csv"
        results_df.to_csv(results_path, index=False)

        print(f"\nSaved results summary to: {results_path}")
        print("\nResults summary:")
        print(results_df.to_string(index=False))
    else:
        print("\nNo models were trained.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)