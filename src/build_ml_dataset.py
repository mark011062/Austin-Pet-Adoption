import os
import re
import sys
from dotenv import load_dotenv

import pandas as pd
from sqlalchemy import create_engine, text

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "55432")
DB_NAME = os.getenv("DB_NAME", "pet_adoption_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")

SOURCE_SCHEMA = "staging"
SOURCE_TABLE = "pet_features"
TARGET_SCHEMA = "ml"
TARGET_TABLE = "adoption_training_data"

ADOPTION_WINDOWS = [7, 14, 30, 60]


def get_engine():
    conn_str = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    return create_engine(conn_str)


# ---------------------------------------------------
# BREED STANDARDIZATION
# ---------------------------------------------------

BREED_REPLACEMENTS = {
    r"\bam pit bull ter\b": "american pit bull terrier",
    r"\bam pit bull\b": "american pit bull terrier",
    r"\bamstaff\b": "american staffordshire terrier",
    r"\bamerican staffordshire\b": "american staffordshire terrier",
    r"\bstaffordshire bull terrier\b": "staffordshire bull terrier",
    r"\bstaffordshire\b": "staffordshire terrier",
    r"\blab\b": "labrador retriever",
    r"\bgolden\b": "golden retriever",
    r"\bgsd\b": "german shepherd",
    r"\bchi\b": "chihuahua",
    r"\bchihuahua shorthair\b": "chihuahua",
    r"\bchihuahua longhair\b": "chihuahua",
    r"\baustralian cattle dog\b": "australian cattle dog",
    r"\bminiature pinscher\b": "min pin",
    r"\bwire hair\b": "wirehaired",
    r"\bplott hound\b": "plott",
    r"\bcoonhound\b": "coonhound",
    r"\brhod ridgeback\b": "rhodesian ridgeback",
    r"\bchow chow\b": "chow chow",
    r"\bpbgv\b": "petit basset griffon vendeen",
    r"\bdomestic shorthair\b": "domestic shorthair",
    r"\bdomestic longhair\b": "domestic longhair",
    r"\bdomestic medium hair\b": "domestic medium hair",
    r"\bunknown\b": "unknown",
}

BREED_GROUP_KEYWORDS = {
    "Pit Bull / Bully": [
        "american pit bull terrier",
        "pit bull",
        "american staffordshire terrier",
        "staffordshire bull terrier",
        "staffordshire terrier",
        "american bulldog",
        "bulldog",
        "bull terrier",
        "boxer",
    ],
    "Labrador / Retriever": [
        "labrador retriever",
        "golden retriever",
        "retriever",
        "flat coat retriever",
        "chesapeake bay retriever",
    ],
    "German Shepherd": [
        "german shepherd",
    ],
    "Small Breed": [
        "chihuahua",
        "dachshund",
        "min pin",
        "miniature pinscher",
        "miniature poodle",
        "miniature schnauzer",
        "lhasa apso",
        "pomeranian",
        "papillon",
        "yorkshire terrier",
        "maltese",
        "shih tzu",
        "pekingese",
        "pug",
    ],
    "Hound": [
        "beagle",
        "basset hound",
        "bloodhound",
        "coonhound",
        "plott",
        "foxhound",
        "greyhound",
        "whippet",
        "saluki",
        "black mouth cur",
        "rhodesian ridgeback",
        "pharaoh hound",
        "harrier",
        "black / tan hound",
        "petit basset griffon vendeen",
    ],
    "Working Breed": [
        "rottweiler",
        "doberman",
        "mastiff",
        "great dane",
        "great pyrenees",
        "saint bernard",
        "bernese mountain dog",
        "newfoundland",
        "akita",
        "husky",
        "siberian husky",
        "alaskan malamute",
        "cane corso",
        "chow chow",
    ],
    "Herding Breed": [
        "border collie",
        "collie",
        "australian shepherd",
        "australian cattle dog",
        "australian kelpie",
        "cattle dog",
        "blue heeler",
        "heeler",
        "belgian malinois",
        "shetland sheepdog",
        "corgi",
    ],
    "Terrier (non-bully)": [
        "jack russell terrier",
        "rat terrier",
        "fox terrier",
        "airedale terrier",
        "west highland white terrier",
        "scottish terrier",
        "cairn terrier",
        "terrier",
    ],
    "Toy Breed": [
        "toy poodle",
        "poodle toy",
        "pomeranian",
        "maltese",
        "papillon",
        "pekingese",
    ],
    "Sporting Breed": [
        "pointer",
        "german shorthaired pointer",
        "setter",
        "spaniel",
        "cocker spaniel",
        "springer spaniel",
        "vizsla",
        "weimaraner",
    ],
    "Cat": [
        "domestic shorthair",
        "domestic longhair",
        "domestic medium hair",
        "siamese",
        "persian",
        "maine coon",
        "ragdoll",
        "bengal",
        "snowshoe",
        "manx",
    ],
    "Other Regional / Primitive": [
        "catahoula",
        "carolina dog",
        "blue lacy",
        "shiba inu",
    ],
}


def clean_text(value):
    if pd.isna(value):
        return "unknown"

    text = str(value).strip().lower()
    if not text:
        return "unknown"

    text = text.replace("&", "/")
    text = re.sub(r"\s+", " ", text)
    return text


def title_case_breed(text):
    if text == "unknown":
        return "Unknown"
    return " ".join(word.capitalize() for word in text.split())


def normalize_breed(raw_breed):
    breed = clean_text(raw_breed)

    breed = breed.replace("\\", "/")
    breed = re.sub(r"\s*/\s*", " / ", breed)
    breed = re.sub(r"\s+", " ", breed).strip()

    for pattern, replacement in BREED_REPLACEMENTS.items():
        breed = re.sub(pattern, replacement, breed)

    breed = re.sub(r"\s+", " ", breed).strip()
    breed = re.sub(r"\b(\w+)( \1\b)+", r"\1", breed)

    return title_case_breed(breed)


def assign_breed_group(breed_clean, animal_type=None):
    breed = clean_text(breed_clean)
    animal = clean_text(animal_type) if animal_type is not None else ""

    if breed == "unknown":
        return "Unknown"

    if animal == "cat":
        return "Cat"

    for cat_keyword in BREED_GROUP_KEYWORDS["Cat"]:
        if cat_keyword in breed:
            return "Cat"

    if breed in {"mix", "unknown mix", "mixed breed", "mixed"}:
        return "Mixed Breed (General)"

    group_priority = [
        "Pit Bull / Bully",
        "Labrador / Retriever",
        "German Shepherd",
        "Small Breed",
        "Hound",
        "Working Breed",
        "Herding Breed",
        "Terrier (non-bully)",
        "Toy Breed",
        "Sporting Breed",
        "Other Regional / Primitive",
    ]

    for group in group_priority:
        for keyword in BREED_GROUP_KEYWORDS[group]:
            if keyword in breed:
                return group

    if "terrier" in breed:
        return "Terrier (non-bully)"

    if "retriever" in breed:
        return "Labrador / Retriever"

    if "shepherd" in breed:
        return "German Shepherd"

    if "poodle" in breed or "schnauzer" in breed:
        return "Small Breed"

    if "mix" in breed:
        return "Mixed Breed (General)"

    return "Other"


# ---------------------------------------------------
# COLOR STANDARDIZATION
# ---------------------------------------------------

COLOR_MAP = {
    "blk": "Black",
    "black": "Black",
    "wht": "White",
    "white": "White",
    "brn": "Brown",
    "brown": "Brown",
    "chocolate": "Brown",
    "tan": "Tan",
    "fawn": "Tan",
    "buff": "Tan",
    "gray": "Gray / Blue",
    "grey": "Gray / Blue",
    "blue": "Gray / Blue",
    "silver": "Gray / Blue",
    "brindle": "Brindle",
    "red": "Orange / Red",
    "orange": "Orange / Red",
    "apricot": "Orange / Red",
    "cream": "Cream / Yellow",
    "yellow": "Cream / Yellow",
    "gold": "Cream / Yellow",
    "golden": "Cream / Yellow",
    "calico": "Tricolor",
    "tricolor": "Tricolor",
    "tri": "Tricolor",
    "liver": "Brown",
}


def normalize_color(raw_color):
    if pd.isna(raw_color):
        return "Unknown"

    color = str(raw_color).strip().lower()
    if not color:
        return "Unknown"

    color = color.replace("&", "/")
    color = re.sub(r"\s+", " ", color)

    if "brindle" in color:
        return "Brindle"

    if "tortie" in color or "torbie" in color:
        return "Tricolor"

    if "lynx" in color:
        return "Gray / Blue"

    if "seal point" in color:
        return "Brown"

    if "flame point" in color:
        return "Cream / Yellow"

    if "lilac point" in color:
        return "Gray / Blue"

    if "point" in color:
        return "Cream / Yellow"

    if "sable" in color:
        return "Brown"

    if "agouti" in color:
        return "Brown"

    if "tricolor" in color or "calico" in color:
        return "Tricolor"

    tokens = re.split(r"[/, ]+", color)
    tokens = [t for t in tokens if t]

    normalized = []
    for token in tokens:
        if token in COLOR_MAP:
            normalized.append(COLOR_MAP[token])

    unique_colors = list(dict.fromkeys(normalized))

    if len(unique_colors) >= 3:
        return "Tricolor"

    if unique_colors:
        return unique_colors[0]

    return "Other"


def apply_breed_color_standardization(df):
    df = df.copy()

    if "breed" not in df.columns:
        raise KeyError("Expected column 'breed' not found in DataFrame.")

    if "color" not in df.columns:
        raise KeyError("Expected column 'color' not found in DataFrame.")

    animal_type_series = (
        df["animal_type"]
        if "animal_type" in df.columns
        else pd.Series([None] * len(df), index=df.index)
    )

    df["breed_clean"] = df["breed"].apply(normalize_breed)
    df["breed_group"] = [
        assign_breed_group(breed_clean, animal_type)
        for breed_clean, animal_type in zip(df["breed_clean"], animal_type_series)
    ]
    df["color_primary"] = df["color"].apply(normalize_color)

    return df


def main():
    engine = get_engine()
    source_fq = f"{SOURCE_SCHEMA}.{SOURCE_TABLE}"
    target_fq = f"{TARGET_SCHEMA}.{TARGET_TABLE}"

    query = f"""
        SELECT
            source_row_id,
            animal_type,
            breed,
            color,
            sex_upon_intake,
            age_upon_intake,
            age_in_days,
            age_bucket,
            has_name,
            intake_type,
            intake_condition,
            intake_year,
            intake_month,
            intake_day,
            intake_dayofweek,
            intake_is_weekend,
            length_of_stay_days AS target_days_to_adoption
        FROM {source_fq}
        WHERE animal_type IN ('Dog', 'Cat')
          AND outcome_type = 'Adoption'
          AND length_of_stay_days IS NOT NULL
          AND length_of_stay_days >= 0;
    """

    print(f"Reading ML source data from {source_fq} ...")
    df = pd.read_sql(query, engine)

    print(f"Loaded {len(df):,} training rows")

    if df.empty:
        raise ValueError("No rows found for ML training dataset.")

    categorical_fill_cols = [
        "animal_type",
        "breed",
        "color",
        "sex_upon_intake",
        "age_bucket",
        "intake_type",
        "intake_condition",
    ]
    for col in categorical_fill_cols:
        if col in df.columns:
            df[col] = df[col].fillna("Unknown")

    if "age_in_days" in df.columns:
        df["age_in_days"] = pd.to_numeric(df["age_in_days"], errors="coerce")

    if "has_name" in df.columns:
        df["has_name"] = df["has_name"].fillna(False).astype(int)
    else:
        df["has_name"] = 0

    if "intake_is_weekend" in df.columns:
        df["intake_is_weekend"] = df["intake_is_weekend"].fillna(False).astype(int)

    df = apply_breed_color_standardization(df)

    df["is_mix"] = df["breed"].str.contains("Mix", case=False, na=False).astype(int)

    df["is_puppy_kitten"] = (
        df["age_in_days"].fillna(999999) < 180
    ).astype(int)

    df["is_senior"] = (
        df["age_in_days"].fillna(-1) >= (365 * 8)
    ).astype(int)

    df["is_summer"] = df["intake_month"].isin([6, 7, 8]).astype(int)
    df["is_holiday_season"] = df["intake_month"].isin([11, 12]).astype(int)

    for window in ADOPTION_WINDOWS:
        df[f"target_adopted_within_{window}_days"] = (
            df["target_days_to_adoption"] <= window
        ).astype(int)

    ordered_cols = [
        "source_row_id",
        "animal_type",
        "breed",
        "breed_clean",
        "breed_group",
        "is_mix",
        "color",
        "color_primary",
        "sex_upon_intake",
        "age_upon_intake",
        "age_in_days",
        "age_bucket",
        "has_name",
        "intake_type",
        "intake_condition",
        "intake_year",
        "intake_month",
        "intake_day",
        "intake_dayofweek",
        "intake_is_weekend",
        "is_puppy_kitten",
        "is_senior",
        "is_summer",
        "is_holiday_season",
        "target_days_to_adoption",
    ]

    for window in ADOPTION_WINDOWS:
        ordered_cols.append(f"target_adopted_within_{window}_days")

    df = df[[c for c in ordered_cols if c in df.columns]]

    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};"))

    print(f"Writing {len(df):,} rows to {target_fq} ...")
    df.to_sql(
        TARGET_TABLE,
        engine,
        schema=TARGET_SCHEMA,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=5000,
    )

    print("Done.")
    print(f"Created table: {target_fq}")
    print("Columns:")
    for col in df.columns:
        print(f" - {col}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)