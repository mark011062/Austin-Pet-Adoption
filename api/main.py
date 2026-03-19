from fastapi import FastAPI
from api.routes.analytics import router as analytics_router
from api.routes.predictions import router as predictions_router

app = FastAPI(
    title="Austin Pet Adoption API",
    description="API layer for pet adoption analytics and predictions",
    version="1.0.0",
)

app.include_router(analytics_router, prefix="/analytics", tags=["Analytics"])
app.include_router(predictions_router, prefix="/predictions", tags=["Predictions"])


@app.get("/")
def root():
    return {
        "message": "Austin Pet Adoption API is running",
        "docs": "/docs",
    }