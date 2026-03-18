from fastapi import FastAPI
from api.routes.analytics import router as analytics_router

app = FastAPI(
    title="Austin Pet Adoption API",
    description="API layer for pet adoption analytics",
    version="1.0.0",
)

app.include_router(analytics_router, prefix="/analytics", tags=["Analytics"])


@app.get("/")
def root():
    return {
        "message": "Austin Pet Adoption API is running",
        "docs": "/docs",
    }