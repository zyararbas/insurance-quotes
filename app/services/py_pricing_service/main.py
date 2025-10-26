from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from models.models import RatingInput, ComprehensiveVehicleSearchRequest
from services.calculations.pricing_orchestrator import PricingOrchestrator
from services.vehicle_search.vehicle_spec_orchestrator import VehicleSpecOrchestrator
import logging
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Coverage Compass AI - Pricing Service",
    description="A FastAPI service to calculate insurance premiums.",
    version="1.0.0"
)

# --- CORS Middleware ---
# This allows the frontend (running on a different port) to communicate with this API.
origins = [
    "http://localhost",
    "http://localhost:3000", # Default port for Next.js dev server
    # Add any other origins you might be running the frontend from
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"], # Allows all methods
    allow_headers=["*"], # Allows all headers
)

# Dummy carrier configuration. In a real app, this would come from a config file or database.
CARRIER_CONFIG = {
    "STATEFARM": {
        "carrier": "STATEFARM",
        "state": "CA",
        # ... other config ...
    }
}

@app.get("/")
def read_root():
    """A simple endpoint to confirm the service is running."""
    return {"message": "Welcome to the Pricing Service API"}

@app.post("/calculate-premium/")
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

        orchestrator = PricingOrchestrator(carrier_config)
        result = orchestrator.calculate_premium(rating_input)
        
        logger.info("--- Final Calculation Output ---")
        logger.info(result)
        return result

    except Exception as e:
        logger.error(f"An error occurred during premium calculation: {e}", exc_info=True)
        # Re-raise as an HTTPException to be sent to the client
        raise HTTPException(status_code=500, detail=f"An internal error occurred: {e}")


@app.get("/vehicle-lookup/")
def vehicle_lookup(
    type: str = Query(..., description="The type of data to look up (e.g., 'years', 'makes', 'models')"),
    year: Optional[int] = Query(None),
    make: Optional[str] = Query(None),
    model: Optional[str] = Query(None),
    series: Optional[str] = Query(None),
    package: Optional[str] = Query(None),
    style: Optional[str] = Query(None),
    engine: Optional[str] = Query(None)
):
    """
    Provides vehicle data for cascading dropdowns on the frontend.
    """
    logger.info(f"--- Vehicle Lookup Request ---")
    logger.info(f"Type: {type}, Year: {year}, Make: {make}, Model: {model}")
    
    try:
        from services.lookup_services.vehicle_lookup_service import VehicleLookupService
        service = VehicleLookupService()
        data = []
        if type == "years":
            data = service.get_years()
        elif type == "makes" and year is not None:
            data = service.get_makes(year)
        elif type == "models" and year is not None and make is not None:
            data = service.get_models(year, make)
        elif type == "series" and year is not None and make is not None and model is not None:
            data = service.get_series(year, make, model)
        elif type == "packages" and year is not None and make is not None and model is not None and series is not None:
            data = service.get_packages(year, make, model, series)
        elif type == "styles" and year is not None and make is not None and model is not None and series is not None and package is not None:
            data = service.get_styles(year, make, model, series, package)
        elif type == "engines" and year is not None and make is not None and model is not None and series is not None and package is not None and style is not None:
            data = service.get_engines(year, make, model, series, package, style)
        elif type == "ratings" and year is not None and make is not None and model is not None and series is not None and package is not None and style is not None and engine is not None:
            data = service.get_rating_groups(year, make, model, series, package, style, engine)
        else:
            raise HTTPException(status_code=400, detail="Invalid 'type' or missing required parameters.")
            
        logger.info(f"Found {len(data)} items for type '{type}'.")
        return {"data": data}
        
    except Exception as e:
        logger.error(f"An error occurred during vehicle lookup: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred during vehicle lookup.")

@app.get("/coverage-limits/")
def get_coverage_limits():
    """
    Returns all available coverage limits and deductible options for the frontend dropdowns.
    """
    logger.info("--- Coverage Limits Request ---")
    
    try:
        from services.lookup_services.coverage_factor_lookup_service import CoverageFactorLookupService
        service = CoverageFactorLookupService(CARRIER_CONFIG["STATEFARM"])
        service.initialize()
        data = service.get_all_coverage_limits()
        
        logger.info(f"Coverage limits loaded successfully.")
        return data
        
    except Exception as e:
        logger.error(f"An error occurred while loading coverage limits: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while loading coverage limits.")

@app.get("/deductible-options/")
def get_deductible_options(
    coverage_type: str = Query(..., description="Coverage type: 'collision' or 'comprehensive'"),
    drg: Optional[int] = Query(None, description="Damage Rating Group for collision"),
    grg: Optional[int] = Query(None, description="Glass Rating Group for comprehensive")
):
    """
    Returns available deductible options for a specific coverage type and vehicle rating group.
    """
    logger.info(f"--- Deductible Options Request: {coverage_type}, DRG: {drg}, GRG: {grg} ---")
    
    try:
        from services.lookup_services.coverage_factor_lookup_service import CoverageFactorLookupService
        service = CoverageFactorLookupService(CARRIER_CONFIG["STATEFARM"])
        service.initialize()
        
        # For now, return all deductible options since the service doesn't have specific methods
        if coverage_type.lower() == "collision":
            data = [
                {'value': 'FULL', 'label': 'Full Coverage'},
                {'value': '50', 'label': '$50'},
                {'value': '100', 'label': '$100'},
                {'value': '200', 'label': '$200'},
                {'value': '250', 'label': '$250'},
                {'value': '500', 'label': '$500'},
                {'value': '1000', 'label': '$1000'},
                {'value': '1000W/20%', 'label': '$1000 w/20%'},
                {'value': '2000', 'label': '$2000'}
            ]
        elif coverage_type.lower() == "comprehensive":
            data = [
                {'value': '50', 'label': '$50'},
                {'value': '100', 'label': '$100'},
                {'value': '200', 'label': '$200'},
                {'value': '250', 'label': '$250'},
                {'value': '500', 'label': '$500'},
                {'value': '1000', 'label': '$1000'},
                {'value': '1000 W/20%', 'label': '$1000 w/20%'},
                {'value': '2000', 'label': '$2000'}
            ]
        else:
            raise HTTPException(status_code=400, detail="Coverage type must be 'collision' or 'comprehensive'")
        
        logger.info(f"Found {len(data)} deductible options for {coverage_type}.")
        return {"data": data}
        
    except Exception as e:
        logger.error(f"An error occurred while loading deductible options: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while loading deductible options.")

@app.get("/coverage-factor/")
def get_coverage_factor(
    coverage_type: str = Query(..., description="Coverage type (BIPD, PD, UM, MPC, COLL, COMP)"),
    limit_or_deductible: str = Query(..., description="The coverage limit or deductible value"),
    drg: Optional[int] = Query(None, description="Damage Rating Group for collision"),
    grg: Optional[int] = Query(None, description="Glass Rating Group for comprehensive")
):
    """
    Returns the coverage factor and calculation details for display.
    """
    logger.info(f"--- Coverage Factor Request: {coverage_type}, Value: {limit_or_deductible}, DRG: {drg}, GRG: {grg} ---")
    
    try:
        from services.lookup_services.coverage_factor_lookup_service import CoverageFactorLookupService
        service = CoverageFactorLookupService(CARRIER_CONFIG["STATEFARM"])
        service.initialize()
        
        # Use the appropriate rating group based on coverage type
        rating_group = drg if coverage_type == "COLL" else grg if coverage_type == "COMP" else None
        
        # Get the coverage factor
        factor = service.get_coverage_factor(coverage_type, limit_or_deductible, rating_group)
        
        result = {
            "coverage_type": coverage_type,
            "limit_or_deductible": limit_or_deductible,
            "rating_group": rating_group,
            "factor": factor,
            "calculation": f"{coverage_type} {limit_or_deductible} = Factor {factor}"
        }
        
        logger.info(f"Coverage factor calculation: {result}")
        return result
        
    except Exception as e:
        logger.error(f"An error occurred while calculating coverage factor: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while calculating coverage factor.")

@app.get("/usage-type-factors/")
def get_usage_type_factors(
    usage_type: str = Query(..., description="Usage type (e.g., 'Pleasure / Work / School', 'Business', 'Farm')")
):
    """
    Returns the factors for all coverage types for a given usage type.
    """
    logger.info(f"--- Usage Type Factors Request: {usage_type} ---")
    
    try:
        from services.lookup_services.driver_factor_lookup_service import DriverFactorLookupService
        from models.models import Usage
        
        # Create a mock usage object
        usage = Usage(
            annual_mileage=10000,
            type=usage_type,
            single_automobile=True
        )
        
        service = DriverFactorLookupService(CARRIER_CONFIG["STATEFARM"])
        service.initialize()
        
        result = service.get_all_usage_type_factors(usage_type)
        
        logger.info(f"Usage type factors: {result}")
        return result
        
    except Exception as e:
        logger.error(f"An error occurred while getting usage type factors: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while getting usage type factors.")

@app.get("/annual-mileage-factors/")
def get_annual_mileage_factors(
    annual_mileage: int = Query(..., description="Annual mileage to find factors for")
):
    """
    Returns the factors for all coverage types for a given annual mileage.
    """
    logger.info(f"--- Annual Mileage Factors Request: {annual_mileage} ---")
    
    try:
        from services.lookup_services.driver_factor_lookup_service import DriverFactorLookupService
        from models.models import Usage
        
        # Create a mock usage object
        usage = Usage(
            annual_mileage=annual_mileage,
            type='Pleasure / Work / School',
            single_automobile=True
        )
        
        service = DriverFactorLookupService(CARRIER_CONFIG["STATEFARM"])
        service.initialize()
        
        result = service.get_all_annual_mileage_factors(annual_mileage)
        
        logger.info(f"Annual mileage factors: {result}")
        return result
        
    except Exception as e:
        logger.error(f"An error occurred while getting annual mileage factors: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while getting annual mileage factors.")

@app.post("/step-by-step-breakdown/")
def get_step_by_step_breakdown(rating_input: RatingInput):
    """
    Returns a detailed step-by-step breakdown of the premium calculation
    in the format expected by the UI.
    """
    logger.info("--- Step-by-Step Breakdown Request ---")
    
    try:
        from services.aggregation_services.coverage_calculation_aggregator import CoverageCalculationAggregator
        
        # Initialize the aggregator
        aggregator = CoverageCalculationAggregator(CARRIER_CONFIG["STATEFARM"])
        aggregator.initialize()
        
        # Get the full calculation result
        full_result = aggregator.calculate_coverage_premiums(rating_input)
        
        # Transform the result to match the UI's expected format
        breakdown = {
            "input": rating_input.dict(),
            "premiums": full_result["premiums"],
            "total_premium": full_result["total_premium"],
            "breakdowns": {
                # Base rates with territory factors
                "base_factors": full_result["breakdowns"]["base_factors"],
                # Driver factors
                "driver_adjustment_factors": full_result["breakdowns"]["driver_adjustment_factors"],
                # Vehicle factors
                "vehicle_factors": full_result["breakdowns"]["vehicle_factors"],
                # Coverage factors
                "coverage_factors": full_result["breakdowns"]["coverage_factors"],
                # Vehicle rating groups
                "vehicle_rating_groups": full_result["breakdowns"]["vehicle_rating_groups"],
                # Discount factors
                "discount_factors": full_result["breakdowns"]["discount_factors"]
            },
            "calculation_summary": full_result["calculation_summary"],
            "metadata": full_result["metadata"]
        }
        
        logger.info("Step-by-step breakdown generated successfully")
        return breakdown
        
    except Exception as e:
        logger.error(f"An error occurred while generating step-by-step breakdown: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while generating step-by-step breakdown.")

# Driver Factor Endpoints

@app.get("/zip-factors/")
def get_zip_factors(
    zip_code: str = Query(..., description="5-digit zip code")
):
    """
    Returns territory factors for a specific zip code.
    """
    logger.info(f"--- Zip Territory Factors Request: {zip_code} ---")
    
    try:
        from services.lookup_services.driver_factor_lookup_service import DriverFactorLookupService
        service = DriverFactorLookupService(CARRIER_CONFIG["STATEFARM"])
        
        result = service.get_zip_factors(zip_code)
        
        logger.info(f"Zip territory factors: {result}")
        return result
        
    except ValueError as e:
        logger.error(f"Zip code not found: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"An error occurred while getting zip factors: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while getting zip factors.")

@app.get("/driver-factors/")
def get_driver_factors(
    marital_status: str = Query(..., description="Marital status (S, M, etc.)"),
    years_licensed: int = Query(..., description="Years driver has been licensed"),
    assigned_driver: str = Query(..., description="Whether this is an assigned driver (Yes/No)")
):
    """
    Returns base driver factors.
    """
    logger.info(f"--- Driver Factors Request: marital={marital_status}, years={years_licensed}, assigned={assigned_driver} ---")
    
    try:
        from services.lookup_services.driver_factor_lookup_service import DriverFactorLookupService
        service = DriverFactorLookupService(CARRIER_CONFIG["STATEFARM"])
        service.initialize()
        
        result = service.get_all_driver_factors(marital_status, years_licensed, assigned_driver)
        
        logger.info(f"Driver factors: {result}")
        return result
        
    except Exception as e:
        logger.error(f"An error occurred while getting driver factors: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while getting driver factors.")

@app.get("/safety-factors/")
def get_safety_factors(
    safety_record_level: int = Query(..., description="Safety record level (0-30)")
):
    """
    Returns safety record factors for a given safety record level.
    """
    logger.info(f"--- Safety Factors Request: level={safety_record_level} ---")
    
    try:
        from services.lookup_services.driver_factor_lookup_service import DriverFactorLookupService
        service = DriverFactorLookupService(CARRIER_CONFIG["STATEFARM"])
        service.initialize()
        
        result = service.get_all_safety_factors(safety_record_level)
        
        logger.info(f"Safety factors: {result}")
        return result
        
    except ValueError as e:
        logger.error(f"Safety record level not found: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"An error occurred while getting safety factors: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while getting safety factors.")

@app.post("/calculate-safety-record/")
def calculate_safety_record(driver_data: dict):
    """
    Calculates comprehensive safety record level from violations with time decay.
    Expects driver data with violations including dates.
    """
    logger.info(f"--- Safety Record Calculation Request: {driver_data} ---")
    
    try:
        from services.calculations.safety_record_service import SafetyRecordService
        from services.lookup_services.driver_factor_lookup_service import DriverFactorLookupService
        from models.models import Driver, Violation
        from datetime import datetime
        
        # Initialize services
        safety_service = SafetyRecordService(CARRIER_CONFIG["STATEFARM"])
        safety_service.initialize()
        
        factor_service = DriverFactorLookupService()
        
        # Parse violations from the request
        violations = []
        if 'violations' in driver_data:
            for v in driver_data['violations']:
                # Default to today's date if no date provided
                violation_date = v.get('date')
                if not violation_date or violation_date == '':
                    violation_date = datetime.now().date()
                else:
                    violation_date = datetime.fromisoformat(violation_date).date()
                
                violation = Violation(
                    type=v['type'],
                    date=violation_date,
                    points_added=v['points_added']
                )
                violations.append(violation)
        
        # Create driver with violations
        driver = Driver(
            driver_id=driver_data.get('driver_id', 'temp'),
            years_licensed=driver_data.get('years_licensed', 10),
            percentage_use=driver_data.get('percentage_use', 100.0),
            assigned_driver=driver_data.get('assigned_driver', True),
            violations=violations
        )
        
        # Calculate comprehensive safety record level with time decay
        calculated_level = safety_service.calculate_safety_record_level(driver)
        violation_details = safety_service.get_violation_details(driver)
        
        # Get safety factors for the calculated level
        safety_factors = factor_service.get_safety_factors(calculated_level)
        
        # Combine results
        result = {
            **safety_factors,
            "calculated_from_violations": True,
            "violation_details": violation_details,
            "safety_calculation": {
                "base_score": 0,  # Clean driver starts at 0
                "violation_points": violation_details['total_violation_points'],
                "final_level": calculated_level,
                "clean_record": violation_details['clean_record']
            }
        }
        
        logger.info(f"Calculated safety record: {result}")
        return result
        
    except Exception as e:
        logger.error(f"An error occurred while calculating safety record: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while calculating safety record.")

@app.get("/percentage-factors/")
def get_percentage_factors(
    assigned_driver: str = Query(..., description="Whether this is an assigned driver (Yes/No)"),
    occasional_driver: str = Query(..., description="Whether this is an occasional driver (Yes/No)")
):
    """
    Returns percentage usage factors.
    """
    logger.info(f"--- Percentage Factors Request: assigned={assigned_driver}, occasional={occasional_driver} ---")
    
    try:
        from services.lookup_services.driver_factor_lookup_service import DriverFactorLookupService
        service = DriverFactorLookupService(CARRIER_CONFIG["STATEFARM"])
        
        result = service.get_percentage_factors(assigned_driver, occasional_driver)
        
        logger.info(f"Percentage factors: {result}")
        return result
        
    except Exception as e:
        logger.error(f"An error occurred while getting percentage factors: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while getting percentage factors.")

@app.get("/years-licensed-factors/")
def get_years_licensed_factors(
    years_licensed: int = Query(..., description="Years driver has been licensed"),
    assigned_driver: str = Query(..., description="Whether this is an assigned driver (Yes/No)")
):
    """
    Returns years licensed factors.
    """
    logger.info(f"--- Years Licensed Factors Request: years={years_licensed}, assigned={assigned_driver} ---")
    
    try:
        from services.lookup_services.driver_factor_lookup_service import DriverFactorLookupService
        service = DriverFactorLookupService(CARRIER_CONFIG["STATEFARM"])
        service.initialize()
        
        result = service.get_all_years_licensed_factors(years_licensed, assigned_driver)
        
        logger.info(f"Years licensed factors: {result}")
        return result
        
    except Exception as e:
        logger.error(f"An error occurred while getting years licensed factors: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while getting years licensed factors.")

@app.get("/percentage-use-factors/")
def get_percentage_use_factors(
    percentage_use: float = Query(..., description="Percentage use by driver (0-100)"),
    assigned_driver: bool = Query(..., description="Whether this is an assigned driver")
):
    """
    Returns percentage use by driver factors.
    """
    logger.info(f"--- Percentage Use Factors Request: percentage={percentage_use}, assigned={assigned_driver} ---")
    
    try:
        from services.lookup_services.driver_factor_lookup_service import DriverFactorLookupService
        service = DriverFactorLookupService(CARRIER_CONFIG["STATEFARM"])
        service.initialize()
        result = service.get_all_percentage_use_factors(percentage_use, assigned_driver)
        
        logger.info(f"Percentage use factors: {result}")
        return result
        
    except Exception as e:
        logger.error(f"An error occurred while getting percentage use factors: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while getting percentage use factors.")

@app.get("/vehicle-search/")
def search_vehicles(
    vin: Optional[str] = Query(None, description="17-character Vehicle Identification Number"),
    make: Optional[str] = Query(None, description="Vehicle make (partial match, case-insensitive)"),
    model: Optional[str] = Query(None, description="Vehicle model (partial match, case-insensitive)"),
    year: Optional[int] = Query(None, description="Vehicle year (exact match)")
):
    """
    Search for vehicles using either VIN or Make/Model/Year combination.
    Returns all matching vehicles with their details and rating groups.
    """
    logger.info(f"--- Vehicle Search Request: vin='{vin}', make='{make}', model='{model}', year='{year}' ---")
    
    try:
        from services.vehicle_search.vehicle_search_service import VehicleSearchService
        
        # Check if VIN search is requested
        if vin:
            if len(vin.strip()) != 17:
                raise HTTPException(status_code=400, detail="VIN must be exactly 17 characters long")
            
            # Use VIN lookup service
            vin_service = VINLookupService()
            vin_data = vin_service.lookup_and_format_vin(vin.strip().upper())
            
            if 'error' in vin_data:
                raise HTTPException(status_code=400, detail=vin_data['error'])
            
            # Convert VIN data to vehicle search format
            vehicle_service = VehicleSearchService()
            results = vehicle_service.search_vehicles(
                make=vin_data.get('make'),
                model=vin_data.get('model'),
                year=int(vin_data.get('year')) if vin_data.get('year') else None
            )
            
            # Add VIN information to results
            for vehicle in results:
                vehicle['vin'] = vin
                vehicle['vin_lookup_data'] = vin_data
            
            logger.info(f"VIN-based vehicle search found {len(results)} matches")
            return {"vehicles": results, "search_type": "vin", "vin_data": vin_data}
        
        # Traditional Make/Model/Year search
        else:
            # Validate that at least one search criteria is provided
            if not make and not model and not year:
                raise HTTPException(status_code=400, detail="Either VIN or at least one search criteria is required (make, model, or year)")
            
            vehicle_service = VehicleSearchService()
            results = vehicle_service.search_vehicles(make=make, model=model, year=year)
            
            logger.info(f"Traditional vehicle search found {len(results)} matches")
            return {"vehicles": results, "search_type": "traditional"}
        
    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        logger.error(f"An error occurred during vehicle search: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred during vehicle search.")

@app.get("/vin-lookup/")
def lookup_vin(
    vin: str = Query(..., description="17-character Vehicle Identification Number")
):
    """
    Look up vehicle information using a VIN from the NHTSA vPIC API.
    Returns detailed vehicle information including make, model, year, and specifications.
    """
    logger.info(f"--- VIN Lookup Request: {vin} ---")
    
    try:
        from services.vehicle_search.vehicle_search_service import VehicleSearchService
        service = VehicleSearchService()
        
        # Validate VIN format
        if not vin or len(vin.strip()) != 17:
            raise HTTPException(status_code=400, detail="VIN must be exactly 17 characters long")
        
        # Perform VIN lookup using the search service
        result = service.search_by_vin_only(vin.strip().upper())
        
        if 'error' in result:
            logger.error(f"VIN lookup failed: {result['error']}")
            raise HTTPException(status_code=400, detail=result['error'])
        
        logger.info(f"VIN lookup successful for VIN: {vin}")
        return result
        
    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        logger.error(f"An error occurred during VIN lookup: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred during VIN lookup.")

@app.post("/ai-interpret-vehicle-results/")
def interpret_vehicle_results(
    vin_data: Optional[Dict[str, Any]] = None,
    search_results: List[Dict[str, Any]] = None,
    additional_info: str = "",
    conversation_history: List[Dict[str, str]] = None
):
    """
    Use OpenAI AI assistant to interpret vehicle search results and find the best match.
    Returns either an exact match or follow-up questions to narrow down the selection.
    """
    logger.info(f"--- AI Vehicle Interpretation Request ---")
    logger.info(f"Results: {len(search_results) if search_results else 0}")
    
    try:
        from services.vehicle_search.ai_assistant_service import AIAssistantService
        
        # Validate inputs
        if not search_results:
            raise HTTPException(status_code=400, detail="search_results is required")
        
        if len(search_results) == 0:
            return {
                "questions": ["Could you provide more details about the vehicle?"]
            }
        
        # Initialize AI service (OpenAI only)
        ai_service = AIAssistantService(provider="openai")
        
        # Get AI interpretation
        result = ai_service.interpret_vehicle_results(
            vin_data=vin_data,
            search_results=search_results,
            additional_info=additional_info,
            conversation_history=conversation_history
        )
        
        logger.info(f"AI interpretation completed: match_found={result.get('match') is not None}")
        return result
        
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"An error occurred during AI interpretation: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred during AI interpretation.")

@app.get("/ai-test-connection/")
def test_ai_connection():
    """
    Test the OpenAI AI assistant API connection.
    """
    logger.info(f"--- OpenAI Connection Test ---")
    
    try:
        from services.vehicle_search.ai_assistant_service import AIAssistantService
        
        ai_service = AIAssistantService(provider="openai")
        result = ai_service.test_connection()
        
        logger.info(f"AI connection test result: {result['status']}")
        return result
        
    except Exception as e:
        logger.error(f"AI connection test failed: {e}", exc_info=True)
        return {
            "status": "error",
            "provider": ai_provider,
            "error": str(e)
        }

@app.get("/ai-supported-providers/")
def get_supported_ai_providers():
    """
    Get list of supported AI providers.
    """
    try:
        from services.vehicle_search.ai_assistant_service import AIAssistantService
        return {
            "providers": AIAssistantService().get_supported_providers(),
            "default": "openai"
        }
    except Exception as e:
        logger.error(f"Error getting supported providers: {e}")
        return {"providers": [], "error": str(e)}

@app.get("/single-auto-factors/")
def get_single_auto_factors(
    single_auto: bool = Query(..., description="Whether this is a single automobile policy")
):
    """
    Returns single automobile factors.
    """
    logger.info(f"--- Single Auto Factors Request: single_auto={single_auto} ---")
    
    try:
        from services.lookup_services.driver_factor_lookup_service import DriverFactorLookupService
        service = DriverFactorLookupService(CARRIER_CONFIG["STATEFARM"])
        service.initialize()
        result = service.get_all_single_auto_factors(single_auto)
        
        logger.info(f"Single auto factors: {result}")
        return result
        
    except Exception as e:
        logger.error(f"An error occurred while getting single auto factors: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while getting single auto factors.")


# Microservices Testing Endpoints

@app.post("/test-driver-adjustment/")
def test_driver_adjustment(rating_input: RatingInput):
    """
    Tests the Driver Adjustment Aggregation Service in isolation.
    Returns only the driver adjustment factors without other calculations.
    """
    logger.info("--- Testing Driver Adjustment Service ---")
    
    try:
        carrier_config = CARRIER_CONFIG.get(rating_input.carrier)
        if not carrier_config:
            raise HTTPException(status_code=400, detail=f"Carrier '{rating_input.carrier}' not supported.")

        orchestrator = PricingOrchestrator(carrier_config)
        result = orchestrator.get_driver_adjustment_factors(rating_input)
        
        logger.info("--- Driver Adjustment Test Results ---")
        logger.info(result)
        return result

    except Exception as e:
        logger.error(f"An error occurred during driver adjustment test: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An internal error occurred: {e}")

@app.post("/test-coverage-breakdown/")
def test_coverage_breakdown(coverage: str = Query(..., description="Coverage to test (e.g., 'BIPD', 'COLL')"), rating_input: RatingInput = None):
    """
    Tests the Coverage Calculation Aggregation Service for a specific coverage.
    Returns detailed breakdown for debugging.
    """
    logger.info(f"--- Testing Coverage Breakdown for {coverage} ---")
    
    try:
        carrier_config = CARRIER_CONFIG.get(rating_input.carrier)
        if not carrier_config:
            raise HTTPException(status_code=400, detail=f"Carrier '{rating_input.carrier}' not supported.")

        orchestrator = PricingOrchestrator(carrier_config)
        result = orchestrator.get_coverage_breakdown(coverage, rating_input)
        
        logger.info(f"--- Coverage Breakdown Test Results for {coverage} ---")
        logger.info(result)
        return result

    except Exception as e:
        logger.error(f"An error occurred during coverage breakdown test: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An internal error occurred: {e}")

@app.post("/test-individual-factors/")
def test_individual_factors(rating_input: RatingInput):
    """
    Tests all individual lookup services without aggregation.
    Returns raw factors for debugging and analysis.
    """
    logger.info("--- Testing Individual Factor Services ---")
    
    try:
        carrier_config = CARRIER_CONFIG.get(rating_input.carrier)
        if not carrier_config:
            raise HTTPException(status_code=400, detail=f"Carrier '{rating_input.carrier}' not supported.")

        orchestrator = PricingOrchestrator(carrier_config)
        result = orchestrator.get_individual_factors(rating_input)
        
        logger.info("--- Individual Factors Test Results ---")
        logger.info(result)
        return result

    except Exception as e:
        logger.error(f"An error occurred during individual factors test: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An internal error occurred: {e}")


@app.post("/comprehensive-vehicle-match/")
def comprehensive_vehicle_match(request: ComprehensiveVehicleSearchRequest):
    """
    Comprehensive API endpoint that handles the complete flow from VIN/manual input to final match.
    
    This endpoint uses the ComprehensiveVehicleSearchService to:
    1. Perform VIN lookup if VIN is provided
    2. Search for vehicles based on make/model/year
    3. Use AI to find exact match or generate questions
    4. Return either a confirmed match or questions to narrow down
    """
    try:
        # Initialize the vehicle spec orchestrator
        orchestrator = VehicleSpecOrchestrator()
        
        # Process the vehicle search request
        result = orchestrator.process_vehicle_request(
            vin=request.vin,
            make=request.make,
            model=request.model,
            year=request.year,
            additional_info=request.additional_info,
            conversation_history=request.conversation_history
        )
        
        # Handle error responses
        if result.get('error'):
            status_code = 400 if result.get('status') in ['incomplete_criteria', 'no_results'] else 500
            raise HTTPException(status_code=status_code, detail=result['error'])
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Comprehensive vehicle match failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An internal error occurred: {e}")


@app.post("/vehicle-spec-orchestrator/")
def vehicle_spec_orchestrator(request: ComprehensiveVehicleSearchRequest):
    """
    Vehicle Spec Orchestrator API endpoint that handles the complete flow with detailed step tracking.
    
    This endpoint uses the VehicleSpecOrchestrator to:
    1. Perform VIN lookup if VIN is provided
    2. Search for vehicles based on make/model/year
    3. Deduplicate vehicle specifications
    4. Use AI to find exact match or generate questions
    5. Return detailed step-by-step results
    """
    try:
        # Import the VehicleSpecOrchestrator
        from services.vehicle_search.vehicle_spec_orchestrator import VehicleSpecOrchestrator
        
        # Initialize the orchestrator
        orchestrator = VehicleSpecOrchestrator()
        
        # Process the vehicle specification request
        result = orchestrator.process_vehicle_request(
            vin=request.vin,
            make=request.make,
            model=request.model,
            year=request.year,
            additional_info=request.additional_info,
            conversation_history=request.conversation_history
        )
        
        # Handle error responses
        if result.get('error'):
            status_code = 400 if result.get('status') in ['incomplete_criteria', 'no_results'] else 500
            raise HTTPException(status_code=status_code, detail=result['error'])
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Vehicle spec orchestrator failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An internal error occurred: {e}")


# Server startup code
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


