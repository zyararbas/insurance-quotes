from typing import Any, Dict, Optional, List

from fastapi import APIRouter, Query
from pydantic import BaseModel

from app.services.py_pricing_service.models.models import RatingInput  # type: ignore
from app.services.py_pricing_service.services.new_pricing_orchestrator import NewPricingOrchestrator  # type: ignore
from app.services.py_pricing_service.services.lookup_services.vehicle_lookup_service import VehicleLookupService
from fastapi import HTTPException

import logging

logger = logging.getLogger(__name__)

router = APIRouter()


class Vehicle(BaseModel):
    year: int
    make: str
    model: str
    series: Optional[str] = ""
    package: Optional[str] = ""
    style: Optional[str] = ""
    engine: Optional[str] = ""


class GenericRequest(BaseModel):
    """
    A generic model for any JSON input.
    The `data` field can hold any JSON object.
    """
    data: Dict[str, Any]


class GenericResponse(BaseModel):
    """
    A generic model for any JSON output.
    The `output` field can hold any JSON object.
    """
    output: Dict[str, Any]


# Dummy carrier configuration. In a real app, this would come from a config file or database.
CARRIER_CONFIG = {
    "STATEFARM": {
        "carrier": "STATEFARM",
        "state": "CA",
        # ... other config ...
    }
}

vehicleLookupService = VehicleLookupService()


@router.post("/quotes", response_model=GenericResponse, tags=["Insurance Quotes"])
async def create_quote(payload: dict):
    """
    Accepts a generic JSON input and returns a generic JSON output.

    This is a placeholder endpoint. In a real application, this would
    contain the logic to process insurance policy data and return a quote.
    """
    # For demonstration, we'll just echo the input data back.
    # A real implementation would have business logic here.
    print([payload])
    return {"output": payload}


@router.post("/calculate-premium/")
def calculate_premium(rating_input: RatingInput):
    """
    Calculates the insurance premium based on the provided rating input from the test page.
    """
    logger.info("--- Received Raw Request for /calculate-premium ---")
    logger.info(rating_input.dict())
    
    try:
        carrier_config = CARRIER_CONFIG.get(rating_input.carrier)
        if not carrier_config:
            raise HTTPException(status_code=400, detail=f"Carrier '{rating_input.carrier}' not supported.")

        orchestrator = NewPricingOrchestrator(carrier_config)
        result = orchestrator.calculate_premium(rating_input)
        
        logger.info("--- Final Calculation Output ---")
        logger.info(result)
        return result

    except Exception as e:
        logger.error(f"An error occurred during premium calculation: {e}", exc_info=True)
        # Re-raise as an HTTPException to be sent to the client
        raise HTTPException(status_code=500, detail=f"An internal error occurred: {e}")


@router.post("/get_vehicle_ratings")
def getVehicleRatings(vehicles: List[Vehicle]):
    """
    Accepts a list of vehicles and returns their corresponding rating groups.
    """
    logger.info(f"--- Vehicle Ratings Request for {len(vehicles)} vehicles ---")
    
    try:
        ratings = []
        for vehicle in vehicles:
            logger.info(f"Looking up ratings for: {vehicle.year} {vehicle.make} {vehicle.model}")
            rating_groups = vehicleLookupService.get_rating_groups(
                year=vehicle.year,
                make=vehicle.make,
                model=vehicle.model,
                series=vehicle.series or "",
                package=vehicle.package or "",
                style=vehicle.style or "",
                engine=vehicle.engine or ""
            )
            ratings.append({"vehicle": vehicle, "ratings": rating_groups}) 
        
        logger.info(f"Found ratings for {len(ratings)} vehicles.")
        return {"data": ratings}
        
    except Exception as e:
        logger.error(f"An error occurred during vehicle lookup: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred during vehicle lookup.")
