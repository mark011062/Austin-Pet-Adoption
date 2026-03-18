import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def get_engine():
    conn_str = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(conn_str)


def main():

    engine = get_engine()

    sql = """
    CREATE SCHEMA IF NOT EXISTS mart;

    DROP TABLE IF EXISTS mart.breed_adoption_summary;

    CREATE TABLE mart.breed_adoption_summary AS
    SELECT
        animal_type,
        breed,
        COUNT(*) AS total_pets,
        SUM(CASE WHEN is_adopted THEN 1 ELSE 0 END) AS adopted_count,
        ROUND(
            SUM(CASE WHEN is_adopted THEN 1 ELSE 0 END)::numeric /
            NULLIF(COUNT(*),0),
            4
        ) AS adoption_rate,
        ROUND(AVG(length_of_stay_days)::numeric,2) AS avg_length_of_stay_days
    FROM staging.pet_features
    WHERE breed IS NOT NULL
    GROUP BY
        animal_type,
        breed
    HAVING COUNT(*) >= 20
    ORDER BY
        adoption_rate DESC;
    """

    with engine.begin() as conn:
        conn.execute(text(sql))

    print("Done.")
    print("Created mart.breed_adoption_summary")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e)
        sys.exit(1)