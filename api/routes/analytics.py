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

        breeds = {"Dog": [], "Cat": []}
        for row in breed_rows:
            animal_type = row["animal_type"]
            breed_clean = row["breed_clean"]
            if animal_type in breeds and breed_clean:
                breeds[animal_type].append(breed_clean)

        colors = {"Dog": [], "Cat": []}
        for row in color_rows:
            animal_type = row["animal_type"]
            color_primary = row["color_primary"]
            if animal_type in colors and color_primary:
                colors[animal_type].append(color_primary)

        intake_types = [
            row["intake_type"]
            for row in intake_type_rows
            if row["intake_type"]
        ]

        intake_conditions = [
            row["intake_condition"]
            for row in intake_condition_rows
            if row["intake_condition"]
        ]

        return {
            "animal_types": ["Dog", "Cat"],
            "breeds": breeds,
            "colors": colors,
            "intake_types": intake_types,
            "intake_conditions": intake_conditions,
        }

    except Exception:
        return {
            "animal_types": ["Dog", "Cat"],
            "breeds": {
                "Dog": [
                    "Labrador Retriever",
                    "German Shepherd",
                    "Pit Bull Mix",
                    "Chihuahua Mix",
                    "Boxer Mix",
                    "Australian Cattle Dog Mix",
                    "Beagle Mix",
                    "Dachshund Mix",
                    "Siberian Husky Mix",
                    "Yorkshire Terrier Mix",
                    "Miniature Poodle Mix",
                    "Rottweiler Mix",
                    "Border Collie Mix",
                    "Catahoula Mix",
                    "American Bulldog Mix",
                ],
                "Cat": [
                    "Domestic Shorthair",
                    "Domestic Medium Hair",
                    "Domestic Longhair",
                    "Siamese Mix",
                    "Tabby Mix",
                    "American Shorthair Mix",
                    "Maine Coon Mix",
                    "Cat Mix",
                ],
            },
            "colors": {
                "Dog": [
                    "Black",
                    "Brown",
                    "White",
                    "Tan",
                    "Gray",
                    "Blue",
                    "Cream",
                    "Tricolor",
                    "Black/White",
                    "Brown/White",
                ],
                "Cat": [
                    "Black",
                    "Gray",
                    "White",
                    "Orange",
                    "Brown",
                    "Cream",
                    "Calico",
                    "Tortoiseshell",
                    "Black/White",
                    "Orange/White",
                ],
            },
            "intake_types": [
                "Stray",
                "Owner Surrender",
                "Public Assist",
                "Wildlife",
                "Euthanasia Request",
                "Abandoned",
            ],
            "intake_conditions": [
                "Normal",
                "Injured",
                "Sick",
                "Aged",
                "Nursing",
                "Neonatal",
                "Other",
            ],
        }