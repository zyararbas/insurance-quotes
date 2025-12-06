from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from app.routes import health, insurance_quotes
from app.services.vector_databases.vehicle_rates_search import initialize_vehicle_rates_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize the singleton
    initialize_vehicle_rates_db()
    yield
    # Shutdown: Clean up if necessary (nothing to do for now)

app = FastAPI(
    title="Insurance Quotes API",
    description="Provides insurance quotes for a given insurance policy information for a given insurance carriers",
    version="0.1.0",
    lifespan=lifespan
)
# Configure CORS
origins = [
    "http://localhost:8001",  # Add your frontend's origin here
    # "https://your-production-frontend.com", # Add other allowed origins
]
# CORS Middleware (if you need to enable CORS for external requests)
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Allows all origins; customize this for security in production
    allow_credentials=True,
    allow_methods=["*"],  # Allows all HTTP methods
    allow_headers=["*"],  # Allows all headers
) 
API_PREFIX = "/insurance-quotes"
# Include the policy routes
app.include_router(health.router, prefix=API_PREFIX,tags=["Auto Insurance Quote Management Health"])
app.include_router(insurance_quotes.router, prefix=API_PREFIX, tags=["Insurance Quotes"])
# app.include_router(california_statefarm_pricing.router, prefix=API_PREFIX, tags=["California State Farm Pricing"])

@app.get("/insurance-quotes")
async def root():
    return {"message": "Welcome to the Insurance Quotes API"}


# Main entry point for running the FastAPI server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8002, reload=True)