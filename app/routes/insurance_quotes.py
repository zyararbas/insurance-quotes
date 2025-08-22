from typing import Any, Dict

from fastapi import APIRouter
from pydantic import BaseModel

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


@router.post("/quotes", response_model=GenericResponse, tags=["Insurance Quotes"])
async def create_quote(payload: dict):
    """
    Accepts a generic JSON input and returns a generic JSON output.

    This is a placeholder endpoint. In a real application, this would
    contain the logic to process insurance policy data and return a quote.
    """
    # For demonstration, we'll just echo the input data back.
    # A real implementation would have business logic here.
    return {"output": payload}