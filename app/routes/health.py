from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.services.health_service import (
    HealthService,
    get_health_service,
)

router = APIRouter(prefix="/health", tags=["Health"])

class HealthResponse(BaseModel):
    status: str

@router.get("", response_model=HealthResponse)
async def health_check(health_service: HealthService = Depends(get_health_service)):
    """
    Checks the health of the service by delegating to the HealthService.
    """
    return health_service.get_health_status()