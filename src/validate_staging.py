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


def run_check(conn, check_name, sql, severity="error", max_bad_rows=0):
    bad_rows = conn.execute(text(sql)).scalar()
    passed = bad_rows <= max_bad_rows

    if passed:
        status = "PASS"
    else:
        status = "WARN" if severity == "warning" else "FAIL"

    if max_bad_rows == 0:
        detail = f"{bad_rows} bad rows"
    else:
        detail = f"{bad_rows} bad rows (allowed up to {max_bad_rows})"

    print(f"[{status}] {check_name} -> {detail}")

    return {
        "check_name": check_name,
        "bad_rows": bad_rows,
        "severity": severity,
        "passed": passed,
        "status": status,
    }


def main():
    engine = get_engine()

    checks = [
        {
            "check_name": "staging.pet_features has rows",
            "sql": """
                SELECT CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END
                FROM staging.pet_features;
            """,
            "severity": "error",
            "max_bad_rows": 0,
        },
        {
            "check_name": "animal_type is not null",
            "sql": """
                SELECT COUNT(*)
                FROM staging.pet_features
                WHERE animal_type IS NULL;
            """,
            "severity": "error",
            "max_bad_rows": 0,
        },
        {
            "check_name": "outcome_type is not null",
            "sql": """
                SELECT COUNT(*)
                FROM staging.pet_features
                WHERE outcome_type IS NULL;
            """,
            "severity": "warning",
            "max_bad_rows": 25,
        },
        {
            "check_name": "animal_type contains only expected values",
            "sql": """
                SELECT COUNT(*)
                FROM staging.pet_features
                WHERE animal_type IS NOT NULL
                  AND animal_type NOT IN ('Dog', 'Cat', 'Bird', 'Other');
            """,
            "severity": "error",
            "max_bad_rows": 0,
        },
        {
            "check_name": "length_of_stay_days is not negative",
            "sql": """
                SELECT COUNT(*)
                FROM staging.pet_features
                WHERE length_of_stay_days < 0;
            """,
            "severity": "error",
            "max_bad_rows": 0,
        },
        {
            "check_name": "outcome_datetime is not earlier than intake_datetime",
            "sql": """
                SELECT COUNT(*)
                FROM staging.pet_features
                WHERE intake_datetime IS NOT NULL
                  AND outcome_datetime IS NOT NULL
                  AND outcome_datetime < intake_datetime;
            """,
            "severity": "error",
            "max_bad_rows": 0,
        },
        {
            "check_name": "is_adopted matches outcome_type",
            "sql": """
                SELECT COUNT(*)
                FROM staging.pet_features
                WHERE (outcome_type = 'Adoption' AND COALESCE(is_adopted, FALSE) = FALSE)
                   OR (outcome_type <> 'Adoption' AND COALESCE(is_adopted, FALSE) = TRUE);
            """,
            "severity": "error",
            "max_bad_rows": 0,
        },
        {
            "check_name": "is_transferred matches outcome_type",
            "sql": """
                SELECT COUNT(*)
                FROM staging.pet_features
                WHERE (outcome_type = 'Transfer' AND COALESCE(is_transferred, FALSE) = FALSE)
                   OR (outcome_type <> 'Transfer' AND COALESCE(is_transferred, FALSE) = TRUE);
            """,
            "severity": "error",
            "max_bad_rows": 0,
        },
        {
            "check_name": "is_died_or_euthanized matches outcome_type",
            "sql": """
                SELECT COUNT(*)
                FROM staging.pet_features
                WHERE (outcome_type IN ('Died', 'Euthanasia') AND COALESCE(is_died_or_euthanized, FALSE) = FALSE)
                   OR (outcome_type NOT IN ('Died', 'Euthanasia') AND COALESCE(is_died_or_euthanized, FALSE) = TRUE);
            """,
            "severity": "error",
            "max_bad_rows": 0,
        },
    ]

    results = []

    with engine.begin() as conn:
        print("Running data quality checks on staging.pet_features...\n")
        for check in checks:
            result = run_check(
                conn=conn,
                check_name=check["check_name"],
                sql=check["sql"],
                severity=check["severity"],
                max_bad_rows=check["max_bad_rows"],
            )
            results.append(result)

    fail_count = sum(1 for r in results if r["status"] == "FAIL")
    warn_count = sum(1 for r in results if r["status"] == "WARN")

    print("\nValidation summary")
    print(f"FAIL checks: {fail_count}")
    print(f"WARN checks: {warn_count}")

    if fail_count > 0:
        print("Validation failed.")
        sys.exit(1)
    else:
        print("Validation passed.")
        sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)