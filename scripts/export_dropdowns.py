# scripts/export_dropdowns.py
import json
import os
import re
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "55432")
DB_NAME = os.getenv("DB_NAME", "pet_adoption_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")


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


conn_str = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)
engine = create_engine(conn_str)

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

payload = {
    "animal_types": ["Dog", "Cat"],
    "breeds": {
        "Dog": sorted(breeds["Dog"]),
        "Cat": sorted(breeds["Cat"]),
    },
    "colors": {
        "Dog": sorted(colors["Dog"]),
        "Cat": sorted(colors["Cat"]),
    },
    "intake_types": sorted({
        str(row["intake_type"]).strip()
        for row in intake_type_rows
        if row["intake_type"]
    }),
    "intake_conditions": sorted({
        str(row["intake_condition"]).strip()
        for row in intake_condition_rows
        if row["intake_condition"]
    }),
}

output_path = Path("data") / "dropdown_options.json"
output_path.parent.mkdir(parents=True, exist_ok=True)

with output_path.open("w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2)

print(f"Wrote {output_path}")
print(f"Dog breeds: {len(payload['breeds']['Dog'])}")
print(f"Cat breeds: {len(payload['breeds']['Cat'])}")
print(f"Dog colors: {len(payload['colors']['Dog'])}")
print(f"Cat colors: {len(payload['colors']['Cat'])}")
print(f"Intake types: {len(payload['intake_types'])}")
print(f"Intake conditions: {len(payload['intake_conditions'])}")