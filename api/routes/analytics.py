import re

from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from api.db import engine

router = APIRouter()


def simplify_breed_for_display(breed_clean: str) -> str:
    if not breed_clean:
        return "Unknown"

    breed = str(breed_clean).strip()
    if not breed:
        return "Unknown"

    breed_lower = breed.lower()

    if breed_lower == "unknown":
        return "Unknown"

    if "/" in breed:
        first_part = breed.split("/")[0].strip()
        if "mix" not in first_part.lower():
            return f"{first_part} Mix"
        return first_part

    if re.search(r"\bmix\b", breed_lower):
        return breed

    return breed


@router.get("/health")
def health_check():
    return {"status": "ok"}


@router.get("/adoptions-by-month")
def adoptions_by_month():
    query = text("""
        SELECT
            t.outcome_year,
            t.outcome_month,
            COUNT(*) AS total_adoptions
        FROM mart.fact_pet_outcomes f
        JOIN mart.dim_time t
            ON f.time_key = t.time_key
        WHERE f.outcome_type = 'Adoption'
        GROUP BY t.outcome_year, t.outcome_month
        ORDER BY t.outcome_year, t.outcome_month;
    """)

    with engine.begin() as conn:
        rows = conn.execute(query).mappings().all()

    return {"data": [dict(row) for row in rows]}


@router.get("/outcomes-by-animal")
def outcomes_by_animal():
    query = text("""
        SELECT
            a.animal_type,
            f.outcome_type,
            COUNT(*) AS total
        FROM mart.fact_pet_outcomes f
        JOIN mart.dim_animal a
            ON f.animal_key = a.animal_key
        GROUP BY a.animal_type, f.outcome_type
        ORDER BY total DESC;
    """)

    with engine.begin() as conn:
        rows = conn.execute(query).mappings().all()

    return {"data": [dict(row) for row in rows]}


@router.get("/top-breeds")
def top_breeds(limit: int = 10):
    query = text("""
        SELECT
            b.breed,
            COUNT(*) AS adoption_count
        FROM mart.fact_pet_outcomes f
        JOIN mart.dim_breed b
            ON f.breed_key = b.breed_key
        WHERE f.outcome_type = 'Adoption'
        GROUP BY b.breed
        ORDER BY adoption_count DESC
        LIMIT :limit;
    """)

    with engine.begin() as conn:
        rows = conn.execute(query, {"limit": limit}).mappings().all()

    return {"data": [dict(row) for row in rows]}


@router.get("/avg-stay-by-animal")
def avg_stay_by_animal():
    query = text("""
        SELECT
            a.animal_type,
            ROUND(AVG(f.length_of_stay_days)::numeric, 2) AS avg_stay_days
        FROM mart.fact_pet_outcomes f
        JOIN mart.dim_animal a
            ON f.animal_key = a.animal_key
        GROUP BY a.animal_type
        ORDER BY avg_stay_days DESC;
    """)

    with engine.begin() as conn:
        rows = conn.execute(query).mappings().all()

    return {"data": [dict(row) for row in rows]}


@router.get("/dropdowns")
def get_dropdowns():
    try:
        breed_query = text("""
            SELECT DISTINCT animal_type, breed_clean
            FROM ml.adoption_training_data
            WHERE animal_type IN ('Dog', 'Cat')
              AND breed_clean IS NOT NULL
              AND TRIM(breed_clean) <> ''
            ORDER BY animal_type, breed_clean;
        """)

        color_query = text("""
            SELECT DISTINCT animal_type, color_primary
            FROM ml.adoption_training_data
            WHERE animal_type IN ('Dog', 'Cat')
              AND color_primary IS NOT NULL
              AND TRIM(color_primary) <> ''
            ORDER BY animal_type, color_primary;
        """)

        intake_type_query = text("""
            SELECT DISTINCT intake_type
            FROM ml.adoption_training_data
            WHERE intake_type IS NOT NULL
              AND TRIM(intake_type) <> ''
            ORDER BY intake_type;
        """)

        intake_condition_query = text("""
            SELECT DISTINCT intake_condition
            FROM ml.adoption_training_data
            WHERE intake_condition IS NOT NULL
              AND TRIM(intake_condition) <> ''
            ORDER BY intake_condition;
        """)

        with engine.begin() as conn:
            breed_rows = conn.execute(breed_query).mappings().all()
            color_rows = conn.execute(color_query).mappings().all()
            intake_type_rows = conn.execute(intake_type_query).mappings().all()
            intake_condition_rows = conn.execute(intake_condition_query).mappings().all()

        breeds = {"Dog": set(), "Cat": set()}
        for row in breed_rows:
            animal_type = row["animal_type"]
            raw_breed = row["breed_clean"]

            if animal_type in breeds and raw_breed:
                cleaned_breed = simplify_breed_for_display(raw_breed)
                if cleaned_breed and cleaned_breed != "Unknown":
                    breeds[animal_type].add(cleaned_breed)

        colors = {"Dog": set(), "Cat": set()}
        for row in color_rows:
            animal_type = row["animal_type"]
            color_primary = row["color_primary"]

            if animal_type in colors and color_primary:
                colors[animal_type].add(str(color_primary).strip())

        intake_types = sorted({
            str(row["intake_type"]).strip()
            for row in intake_type_rows
            if row["intake_type"]
        })

        intake_conditions = sorted({
            str(row["intake_condition"]).strip()
            for row in intake_condition_rows
            if row["intake_condition"]
        })

        return {
            "animal_types": ["Dog", "Cat"],
            "breeds": {
                "Dog": sorted(breeds["Dog"]),
                "Cat": sorted(breeds["Cat"]),
            },
            "colors": {
                "Dog": sorted(colors["Dog"]),
                "Cat": sorted(colors["Cat"]),
            },
            "intake_types": intake_types,
            "intake_conditions": intake_conditions,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Dropdown query failed: {str(e)}")