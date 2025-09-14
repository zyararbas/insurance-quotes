"""
California State Farm Pricing Routes

API endpoints for California State Farm insurance pricing calculations.
"""

from typing import Dict, Any
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from app.services.california_statefarm_service import CaliforniaStateFarmPricingService

router = APIRouter()

# Initialize the pricing service
pricing_service = CaliforniaStateFarmPricingService()


class DriverInfo(BaseModel):
    """Driver information model."""
    age: int = Field(..., ge=16, le=100, description="Driver age")
    location: str = Field(..., description="Driver location in California")
    driving_record: str = Field(default="good", description="Driving record: excellent, good, fair, poor")
    safe_driver: bool = Field(default=False, description="Qualifies for safe driver discount")
    years_licensed: int = Field(..., ge=0, description="Years licensed to drive")


class VehicleInfo(BaseModel):
    """Vehicle information model."""
    year: int = Field(..., ge=1900, le=2025, description="Vehicle year")
    make: str = Field(..., description="Vehicle make")
    model: str = Field(..., description="Vehicle model")
    vin: str = Field(..., description="Vehicle identification number")
    value: float = Field(..., gt=0, description="Vehicle value")


class CoverageOptions(BaseModel):
    """Coverage options model."""
    liability_limits: str = Field(default="15/30/5", description="Liability coverage limits")
    collision_deductible: int = Field(default=500, description="Collision deductible amount")
    comprehensive_deductible: int = Field(default=500, description="Comprehensive deductible amount")
    uninsured_motorist: bool = Field(default=True, description="Uninsured motorist coverage")
    medical_payments: bool = Field(default=True, description="Medical payments coverage")


class CaliforniaStateFarmQuoteRequest(BaseModel):
    """Request model for California State Farm insurance quotes."""
    driver_info: DriverInfo
    vehicle_info: VehicleInfo
    coverage_options: CoverageOptions
    multi_policy: bool = Field(default=False, description="Has multiple policies with State Farm")
    multi_car: bool = Field(default=False, description="Insuring multiple vehicles")


class QuoteResponse(BaseModel):
    """Response model for insurance quotes."""
    quote_id: str
    carrier: str
    state: str
    premium_amount: float
    coverage_breakdown: Dict[str, float]
    discounts_applied: list
    total_discounts: float
    effective_date: str
    quote_expires: str
    policy_data: Dict[str, Any]


@router.post("/california-statefarm/quote", response_model=QuoteResponse, tags=["California State Farm Pricing"])
async def get_california_statefarm_quote(request: CaliforniaStateFarmQuoteRequest):
    """
    Get a California State Farm insurance quote.
    
    This endpoint calculates insurance premiums for California State Farm policies
    based on driver information, vehicle details, and coverage options.
    
    Args:
        request: California State Farm quote request with driver, vehicle, and coverage info
        
    Returns:
        QuoteResponse: Detailed quote information including premium amount and breakdown
        
    Raises:
        HTTPException: If quote calculation fails
    """
    try:
        # Convert Pydantic model to dictionary
        policy_data = request.dict()
        
        # Calculate the quote
        quote_result = pricing_service.calculate_quote(policy_data)
        
        # Check for errors in the quote calculation
        if "error" in quote_result:
            raise HTTPException(status_code=400, detail=quote_result["error"])
        
        return quote_result
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid request data: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/california-statefarm/coverage-options", tags=["California State Farm Pricing"])
async def get_coverage_options():
    """
    Get available coverage options for California State Farm policies.
    
    Returns:
        Dict: Available coverage options and their descriptions
    """
    return {
        "liability_limits": {
            "15/30/5": "California minimum coverage",
            "25/50/25": "Standard coverage",
            "50/100/50": "Enhanced coverage",
            "100/300/100": "Premium coverage"
        },
        "deductible_options": {
            "collision": [250, 500, 1000, 2500],
            "comprehensive": [250, 500, 1000, 2500]
        },
        "additional_coverage": {
            "uninsured_motorist": "Protection against uninsured drivers",
            "medical_payments": "Medical expense coverage",
            "rental_reimbursement": "Rental car coverage",
            "roadside_assistance": "Emergency roadside service"
        }
    }


@router.get("/california-statefarm/discounts", tags=["California State Farm Pricing"])
async def get_available_discounts():
    """
    Get available discounts for California State Farm policies.
    
    Returns:
        Dict: Available discounts and their descriptions
    """
    return {
        "safe_driver": {
            "description": "Discount for drivers with clean driving records",
            "savings": "Up to 10%"
        },
        "multi_car": {
            "description": "Discount for insuring multiple vehicles",
            "savings": "Up to 5%"
        },
        "multi_policy": {
            "description": "Discount for bundling multiple insurance policies",
            "savings": "Up to 10%"
        },
        "good_student": {
            "description": "Discount for students with good grades",
            "savings": "Up to 15%"
        },
        "defensive_driving": {
            "description": "Discount for completing defensive driving course",
            "savings": "Up to 5%"
        }
    }


@router.get("/california-statefarm/quote/{quote_id}", tags=["California State Farm Pricing"])
async def get_quote_by_id(quote_id: str):
    """
    Retrieve a previously generated quote by ID.
    
    Args:
        quote_id: The unique quote identifier
        
    Returns:
        Dict: Quote information if found
        
    Raises:
        HTTPException: If quote is not found
    """
    # In a real implementation, this would query a database
    # For now, return a placeholder response
    if not quote_id.startswith("CA-SF-"):
        raise HTTPException(status_code=400, detail="Invalid quote ID format")
    
    return {
        "message": f"Quote {quote_id} retrieved successfully",
        "note": "This is a placeholder response. In production, this would query the database for the actual quote data."
    }
