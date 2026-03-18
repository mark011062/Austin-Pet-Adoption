from fastapi import APIRouter
from sqlalchemy import text
from api.db import engine

router = APIRouter()


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