from fastapi import FastAPI

from app.routes import health

app = FastAPI(
    title="Insurance Quotes API",
    description="Provides insurance quotes for a given insurance policy information for a given insurance carriers",
    version="0.1.0",
)

app.include_router(health.router)

@app.get("/")
async def root():
    return {"message": "Welcome to the Insurance Quotes API"}


# Main entry point for running the FastAPI server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8002, reload=True)