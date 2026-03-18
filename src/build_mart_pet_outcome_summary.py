import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "55432")
DB_NAME = os.getenv("DB_NAME", "pet_adoption_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def get_engine():
    conn_str = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(conn_str)


def main():
    engine = get_engine()

    sql = """
    CREATE SCHEMA IF NOT EXISTS mart;

    DROP TABLE IF EXISTS mart.pet_outcome_summary;

    CREATE TABLE mart.pet_outcome_summary AS
    SELECT
        outcome_year,
        outcome_month,
        animal_type,
        COUNT(*) AS total_pets,
        SUM(CASE WHEN is_adopted THEN 1 ELSE 0 END) AS adopted_count,
        SUM(CASE WHEN is_transferred THEN 1 ELSE 0 END) AS transferred_count,
        SUM(CASE WHEN is_died_or_euthanized THEN 1 ELSE 0 END) AS died_or_euthanized_count,
        ROUND(AVG(length_of_stay_days)::numeric, 2) AS avg_length_of_stay_days,
        ROUND(
            SUM(CASE WHEN is_adopted THEN 1 ELSE 0 END)::numeric
            / NULLIF(COUNT(*), 0),
            4
        ) AS adoption_rate,
        ROUND(
            SUM(CASE WHEN is_transferred THEN 1 ELSE 0 END)::numeric
            / NULLIF(COUNT(*), 0),
            4
        ) AS transfer_rate,
        ROUND(
            SUM(CASE WHEN is_died_or_euthanized THEN 1 ELSE 0 END)::numeric
            / NULLIF(COUNT(*), 0),
            4
        ) AS died_or_euthanized_rate
    FROM staging.pet_features
    WHERE outcome_year IS NOT NULL
      AND outcome_month IS NOT NULL
      AND animal_type IS NOT NULL
    GROUP BY
        outcome_year,
        outcome_month,
        animal_type
    ORDER BY
        outcome_year,
        outcome_month,
        animal_type;
    """

    with engine.begin() as conn:
        conn.execute(text(sql))

    print("Done.")
    print("Created table: mart.pet_outcome_summary")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)