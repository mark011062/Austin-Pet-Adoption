import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine


def get_db_url() -> str:
    user = "postgres"
    password = "password"
    host = "127.0.0.1"
    port = "5433"
    db = "pet_adoption_db"

    return "postgresql+psycopg2://postgres:password@127.0.0.1:55432/pet_adoption_db"


def main() -> None:
    project_root = Path(__file__).resolve().parent.parent
    csv_path = project_root / "data" / "raw" / "aac_intakes_outcomes.csv"

    if not csv_path.exists():
        raise FileNotFoundError(f"Could not find file: {csv_path}")

    print(f"Reading CSV from: {csv_path}")
    df = pd.read_csv(csv_path)

    print(f"Rows: {len(df):,}")
    print(f"Columns: {len(df.columns)}")
    print(get_db_url())

    engine = create_engine(get_db_url())

    table_name = "raw_aac_intakes_outcomes"

    print(f"Loading data into Postgres table: {table_name}")
    df.to_sql(
        table_name,
        engine,
        if_exists="replace",
        index=False,
        chunksize=5000,
        method="multi",
    )

    print("Ingest complete.")


if __name__ == "__main__":
    main()