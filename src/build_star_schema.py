import os
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

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

    statements = [
        "CREATE SCHEMA IF NOT EXISTS mart;",
        "DROP TABLE IF EXISTS mart.fact_pet_outcomes;",
        "DROP TABLE IF EXISTS mart.dim_time;",
        "DROP TABLE IF EXISTS mart.dim_breed;",
        "DROP TABLE IF EXISTS mart.dim_animal;",

        """
        CREATE TABLE mart.dim_animal AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY animal_type, sex_upon_intake, age_bucket) AS animal_key,
            animal_type,
            sex_upon_intake,
            age_bucket,
            is_dog,
            is_cat
        FROM (
            SELECT DISTINCT
                animal_type,
                sex_upon_intake,
                age_bucket,
                is_dog,
                is_cat
            FROM staging.pet_features
        ) d;
        """,

        """
        CREATE TABLE mart.dim_breed AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY breed) AS breed_key,
            breed
        FROM (
            SELECT DISTINCT breed
            FROM staging.pet_features
            WHERE breed IS NOT NULL
        ) d;
        """,

        """
        CREATE TABLE mart.dim_time AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY full_date) AS time_key,
            outcome_year,
            outcome_month,
            outcome_day,
            outcome_dayofweek,
            outcome_is_weekend,
            full_date
        FROM (
            SELECT DISTINCT
                outcome_year,
                outcome_month,
                outcome_day,
                outcome_dayofweek,
                outcome_is_weekend,
                MAKE_DATE(outcome_year::int, outcome_month::int, outcome_day::int) AS full_date
            FROM staging.pet_features
            WHERE outcome_year IS NOT NULL
              AND outcome_month IS NOT NULL
              AND outcome_day IS NOT NULL
        ) d;
        """,

        """
        CREATE TABLE mart.fact_pet_outcomes AS
        SELECT
            pf.source_row_id,
            da.animal_key,
            db.breed_key,
            dt.time_key,
            pf.intake_datetime,
            pf.outcome_datetime,
            pf.intake_type,
            pf.intake_condition,
            pf.outcome_type,
            pf.outcome_subtype,
            pf.length_of_stay_days,
            pf.is_adopted,
            pf.is_transferred,
            pf.is_died_or_euthanized
        FROM staging.pet_features pf
        LEFT JOIN mart.dim_animal da
            ON pf.animal_type IS NOT DISTINCT FROM da.animal_type
           AND pf.sex_upon_intake IS NOT DISTINCT FROM da.sex_upon_intake
           AND pf.age_bucket IS NOT DISTINCT FROM da.age_bucket
        LEFT JOIN mart.dim_breed db
            ON pf.breed IS NOT DISTINCT FROM db.breed
        LEFT JOIN mart.dim_time dt
            ON pf.outcome_year IS NOT DISTINCT FROM dt.outcome_year
           AND pf.outcome_month IS NOT DISTINCT FROM dt.outcome_month
           AND pf.outcome_day IS NOT DISTINCT FROM dt.outcome_day;
        """,

        "ALTER TABLE mart.dim_animal ADD PRIMARY KEY (animal_key);",
        "ALTER TABLE mart.dim_breed ADD PRIMARY KEY (breed_key);",
        "ALTER TABLE mart.dim_time ADD PRIMARY KEY (time_key);",
        "ALTER TABLE mart.fact_pet_outcomes ADD PRIMARY KEY (source_row_id);",

        """
        ALTER TABLE mart.fact_pet_outcomes
        ADD CONSTRAINT fk_fact_animal
        FOREIGN KEY (animal_key) REFERENCES mart.dim_animal(animal_key);
        """,

        """
        ALTER TABLE mart.fact_pet_outcomes
        ADD CONSTRAINT fk_fact_breed
        FOREIGN KEY (breed_key) REFERENCES mart.dim_breed(breed_key);
        """,

        """
        ALTER TABLE mart.fact_pet_outcomes
        ADD CONSTRAINT fk_fact_time
        FOREIGN KEY (time_key) REFERENCES mart.dim_time(time_key);
        """,
    ]

    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))

    print("Done.")
    print("Created tables:")
    print(" - mart.dim_animal")
    print(" - mart.dim_breed")
    print(" - mart.dim_time")
    print(" - mart.fact_pet_outcomes")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)