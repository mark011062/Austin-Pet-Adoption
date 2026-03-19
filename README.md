# Austin Pet Adoption Data Pipeline

This project is an end-to-end data engineering pipeline built on the Austin Animal Center dataset. It ingests raw CSV data, transforms and validates it, and loads it into a dimensional warehouse modeled using a star schema for analytics.

The pipeline is orchestrated using a DAG-style workflow with explicit task dependencies, similar to Apache Airflow.

---

## Project Overview

This project demonstrates core data engineering concepts:

- Data ingestion from raw CSV
- Data cleaning and feature engineering
- Data quality validation
- Dimensional modeling (star schema)
- Analytical data marts
- DAG-style pipeline orchestration
- Query performance optimization with indexes
- API layer for data access
- Machine learning for adoption prediction

---

## Architecture

```
Raw CSV
   ↓
raw.raw_aac_intakes_outcomes
   ↓
staging.pet_features
   ↓
validation checks
   ↓
mart.dim_* + mart.fact_pet_outcomes
   ↓
mart summary tables
   ↓
ml.adoption_training_data
   ↓
ML models
   ↓
FastAPI (analytics + predictions)
```

---

## Data Model (Star Schema)

### Fact Table
- `mart.fact_pet_outcomes`

### Dimension Tables
- `mart.dim_animal`
- `mart.dim_breed`
- `mart.dim_time`

---

## Pipeline Flow (DAG)

```
ingest_data
   ↓
prepare_features
   ↓
validate_staging
   ↓
build_star_schema
   ├── build_mart_pet_outcome_summary
   ├── build_mart_breed_adoption_summary
   └── build_ml_dataset
```

---

## API Layer

This project includes a FastAPI service that exposes both analytics and machine learning predictions.

### Run the API

```bash
uvicorn api.main:app --reload
```

### API Docs

```
http://127.0.0.1:8000/docs
```

---

### Analytics Endpoints

- `GET /analytics/adoptions-by-month`
- `GET /analytics/outcomes-by-animal`
- `GET /analytics/top-breeds`
- `GET /analytics/avg-stay-by-animal`

---

### Prediction Endpoints

- `GET /predictions/health`
  - Health check for model loading

- `POST /predictions/adoption-within-30-days`
  - Predicts whether an animal will be adopted within 30 days

---

### Example Prediction Request

```json
{
  "animal_type": "Dog",
  "breed_group": "Labrador",
  "is_mix": 1,
  "color": "Black/White",
  "sex_upon_intake": "Neutered Male",
  "age_bucket": "young",
  "age_in_days": 365,
  "has_name": 1,
  "name_length": 1,
  "is_puppy_kitten": 0,
  "is_senior": 0,
  "intake_type": "Stray",
  "intake_condition": "Normal",
  "intake_year": 2017,
  "intake_month": 6,
  "intake_day": 15,
  "intake_dayofweek": 3,
  "intake_is_weekend": 0,
  "is_summer": 1,
  "is_holiday_season": 0
}
```

### Example Prediction Response

```json
{
  "prediction_adopted_within_30_days": 1,
  "probability_adopted_within_30_days": 0.8022
}
```

---

## Machine Learning

### Regression Model
Predicts number of days until adoption.

- Model: `HistGradientBoostingRegressor`
- MAE: `19.60`
- RMSE: `51.76`
- R²: `0.0857`

---

### Classification Model (Primary)

Predicts whether an animal will be adopted within 30 days.

- Model: `HistGradientBoostingClassifier`
- Accuracy: `0.8176`
- Precision: `0.8208`
- Recall: `0.9524`
- F1 Score: `0.8817`

---

## Project Structure

```
Austin-Pet-Adoption/
│
├── data/
├── src/
├── api/
├── models/
├── run_pipeline.py
├── README.md
```

---

## How to Run

### Start Postgres
```
docker start austin_pet_postgres
```

### Run pipeline
```
python run_pipeline.py
```

### Run API
```
uvicorn api.main:app --reload
```

---

## What This Project Demonstrates

- End-to-end data engineering pipeline
- Star schema modeling
- Data validation
- DAG orchestration
- API development with FastAPI
- Machine learning integration
- Model serving via API

---

## Future Improvements

- Airflow / Prefect orchestration
- dbt transformations
- dashboards (Tableau / Power BI)
- CI/CD pipeline
- real-time predictions

---

## Author

Mark Young  
Data Analyst → Data Engineer