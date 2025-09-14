from typing import Any, Dict

from fastapi import APIRouter
from pydantic import BaseModel

from app.services.py_pricing_service.models.models import RatingInput  # type: ignore
from app.services.py_pricing_service.services.new_pricing_orchestrator import NewPricingOrchestrator  # type: ignore
from fastapi import HTTPException
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


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
