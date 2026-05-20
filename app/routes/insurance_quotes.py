from typing import Any, Dict, Optional, List
import time
from datetime import date

from fastapi import APIRouter, Query
from fastapi import HTTPException

from pydantic import BaseModel, Field

from app.models.models import ComprehensiveVehicleSearchRequest, RatingInput  # type: ignore
from app.services.calculations.pricing_orchestrator import PricingOrchestrator
from app.services.lookup_services.vehicle_lookup_service import VehicleLookupService
from app.services.vehicle_search.vehicle_spec_orchestrator import VehicleSpecOrchestrator
from app.routes.adapter_service import AdapterService
from app.services.calculations.home.home_insurance import calculate_home_insurance, get_deductible_factor
from app.services.calculations.home.cdi_lookup import CDILookupService
from app.services.calculations.home.cdi_location import resolve_zip_info
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
    },
     "aaa": {
        "carrier": "STATEFARM",
        "state": "CA",
        # ... other config ...
    },
     "farmers": {
        "carrier": "STATEFARM",
        "state": "CA",
        # ... other config ...
    },
     "geico": {
        "carrier": "STATEFARM",
        "state": "CA",
        # ... other config ...
    },
    "generic": {
        "carrier": "STATEFARM",
        "state": "CA",
        # ... other config ...
    }
}

CARRIER_CONFIG_STATEFARM_CA = {
            "carrier": "STATEFARM",
            "state": "CA",
            # ... other config ...
         }

vehicleLookupService = VehicleLookupService() 
adapter_service = AdapterService()
pricing_orchestrator = PricingOrchestrator(CARRIER_CONFIG_STATEFARM_CA)

@router.post("/vehicle-spec-orchestrator/")
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

        
        # Initialize the orchestrator
        orchestrator = VehicleSpecOrchestrator()
        
        start_time = time.time()
        # Process the vehicle specification request
        result = orchestrator.process_vehicle_request(
            vin=request.vin,
            make=request.make,
            model=request.model,
            year=request.year,
            additional_info=request.additional_info,
            conversation_history=request.conversation_history
        )
        duration = time.time() - start_time
        logger.info(f"API /vehicle-spec-orchestrator/ took {duration:.4f} seconds")
        
        # Handle error responses
        if result.get('error'):
            status_code = 400 if result.get('status') in ['incomplete_criteria', 'no_results'] else 500
            raise HTTPException(status_code=status_code, detail=result['error'])
        
        return result
        
    except HTTPException:
        raise HTTPException(status_code=400, detail=f"Could not extract necessary Vehicle information")
    except Exception as e:
        logger.error(f"Vehicle spec orchestrator failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An internal error occurred: {e}")
payload_debug = {'policy_details': {'success': True, 'data': {'policy': {'policyNumber': 'CAAS102222467', 'policyId': 'CAAS102222467_2025-04-30_2026-04-30', 'startDate': '2025-04-30', 'endDate': '2026-04-30', 'totalPremium': '$6,194.28', 'carrier': 'aaa', 'documentType': 'AUTO_INSURANCE_POLICY', 'address': {'streetAddress': '749 DELAWARE AVE', 'addressLocality': 'SAN JOSE', 'addressRegion': 'CA', 'postalCode': '95123-5418'}}, 'company': {'name': 'CSAA Insurance Exchange', 'address': {'streetAddress': 'P.O.Box 22221', 'addressLocality': 'Oakland', 'addressRegion': 'CA', 'postalCode': '94623-2221'}, 'supportCallNumber': '800.922.8228'}, 'drivers': [{'givenName': 'Aykut', 'familyName': 'Yararbas'}, {'givenName': 'Onur', 'familyName': 'Yararbas', 'additionalName': 'Tuna'}, {'givenName': 'Zubeyde', 'familyName': 'Yararbas'}], 'vehicles': [{'brand': 'AUDI', 'model': 'WAGON', 'modelDate': '2011', 'vehicleIdentificationNumber': 'WA1DKAFP2BA076333', 'bodyType': 'WAGON 4 DOOR', 'itemNumber': 1}, {'brand': 'RIVIAN', 'modelDate': '2025', 'vehicleIdentificationNumber': '7PDSGBBA9SN063117', 'bodyType': 'SUV', 'itemNumber': 2}, {'brand': 'TOYOTA', 'model': 'SEDAN', 'modelDate': '2002', 'vehicleIdentificationNumber': '1NXBR12E62Z573165', 'bodyType': 'SEDAN 4 DOOR', 'itemNumber': 3}], 'coverages': [{'coverageName': 'Bodily Injury', 'limitPerPerson': '500,000', 'limitPerOccurrence': '500,000'}, {'coverageName': 'Medical Payments', 'limitPerPerson': '25,000'}, {'coverageName': 'Property Damage', 'limitPerPerson': '100,000', 'limitPerOccurrence': '100,000'}, {'coverageName': 'Uninsured Motorist Property Damage', 'limitPerPerson': '3,500'}, {'coverageName': 'Uninsured Motorists', 'limitPerPerson': '300,000', 'limitPerOccurrence': '500,000'}], 'vehicleCoverages': [{'vehicleItemNumber': 1, 'premiumAmount': '$257', 'coverageStatus': 'Active'}, {'vehicleItemNumber': 1, 'premiumAmount': '$33', 'coverageStatus': 'Active'}, {'vehicleItemNumber': 1, 'premiumAmount': '$367', 'coverageStatus': 'Active'}, {'vehicleItemNumber': 1, 'premiumAmount': '$93', 'coverageStatus': 'Active'}, {'vehicleItemNumber': 1, 'coverageStatus': 'Included'}, {'vehicleItemNumber': 2, 'deductibleLimit': '$50/30', 'premiumAmount': '$52', 'coverageStatus': 'Active'}, {'vehicleItemNumber': 2, 'deductibleLimit': '1,000', 'premiumAmount': '$1,153', 'coverageStatus': 'Active'}, {'vehicleItemNumber': 2, 'deductibleLimit': '1,000', 'premiumAmount': '$328', 'coverageStatus': 'Active'}, {'vehicleItemNumber': 2, 'premiumAmount': '$102', 'coverageStatus': 'Active'}, {'vehicleItemNumber': 2, 'premiumAmount': '$148', 'coverageStatus': 'Active'}, {'vehicleItemNumber': 2, 'premiumAmount': '$223', 'coverageStatus': 'Active'}, {'vehicleItemNumber': 2, 'premiumAmount': '$25', 'coverageStatus': 'Active'}, {'vehicleItemNumber': 2, 'premiumAmount': '$395', 'coverageStatus': 'Active'}, {'vehicleItemNumber': 2, 'coverageStatus': 'Included'}, {'vehicleItemNumber': 3, 'premiumAmount': '$1,161', 'coverageStatus': 'Active'}, {'vehicleItemNumber': 3, 'premiumAmount': '$1,168', 'coverageStatus': 'Active'}, {'vehicleItemNumber': 3, 'premiumAmount': '$146', 'coverageStatus': 'Active'}, {'vehicleItemNumber': 3, 'premiumAmount': '$538', 'coverageStatus': 'Active'}, {'vehicleItemNumber': 3, 'coverageStatus': 'Included'}], 'discounts': [{'discountName': 'Good Driver'}, {'discountName': 'Good Student'}, {'discountName': 'Mature Driver'}, {'discountName': 'Multi Car'}, {'discountName': 'Multi Policy Home'}, {'discountName': 'New Driver'}], 'surcharges': [{'surchargeName': 'CA Special Fraud Assessment Fee', 'surchargeAmount': '$5.28'}, {'surchargeName': 'CA Surcharge', 'surchargeAmount': '$0'}], '_metadata': {'source': 'triplestore', 'query_type': 'direct_sparql_to_json', 'policy_number': 'CAAS102222467', 'retrieved_at': '"2025-10-31 06:42:27.335014"'}}}, 'additional_info': {'drivers': [{'firstName': 'Aykut', 'middleName': '', 'lastName': 'YARARBAS', 'dob': '01/02/2001', 'gender': 'Male', 'education': "Master's Degree", 'ageLicensed': '18'}, {'firstName': 'ZUBEYDE', 'middleName': '', 'lastName': 'YARARBAS', 'dob': '01/02/2001', 'gender': 'Female', 'education': "Master's Degree", 'ageLicensed': '18'}, {'firstName': 'Onur', 'middleName': 'Tuna', 'lastName': 'Yararbas', 'dob': '01/02/2001', 'gender': 'Male', 'education': "Master's Degree", 'ageLicensed': '18'}], 'vehicles': [{'vehicle': {'vin': 'WA1DKAFP2BA076333', 'id': 'trim_engine', 'answer': '3.2 PREMIUM PLUS AWD'}}, {'vehicle': {'vin': '1NXBR12E62Z573165', 'id': 'trim_level', 'answer': 'CE'}}], 'general_questions': {'housingType': 'Own Home', 'housingDuration': 'No', 'insuredDuration': '3+ yrs', 'accidentsOrTickets': 'No', 'vehicles': []}}}

@router.post("/quotes", tags=["Insurance Quotes"])
async def create_quote(payload: Dict[str, Any]):
    """
    Accepts a raw insurance policy payload and transforms it into a list of
    structured RatingInput objects, one for each vehicle in the policy.
    """
    try:
        logging.info(f"Paylod  |{payload}|")
        rating_inputs = adapter_service.create_rating_inputs_from_payload(payload)
        logging.info(f" Rating inputs |{rating_inputs}|")
        results = []
        for rating_input in rating_inputs:
            result = pricing_orchestrator.calculate_premium(rating_input)
            if result:
                results.append(result)
        
        return results

    except Exception as e:
        logger.error(f"An error occurred during payload transformation: {e} - payload {payload}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An internal error occurred during transformation: {e}")


# ---------------------------------------------------------------------------
# Home Insurance Quote endpoint
# ---------------------------------------------------------------------------

_CDI_VALID_AMOUNTS = {
    "HOMEOWNERS":              [250000, 375000, 500000, 625000, 750000, 1000000],
    "CONDOMINIUM":             [25000, 50000, 75000, 100000],
    "RENTERS":                 [15000, 25000, 30000, 50000, 70000],
    "MOBILEHOME":              [50000, 100000],
    "EARTHQUAKE_SINGLE_FAMILY":[250000, 375000, 500000, 625000, 750000, 1000000],
    "EARTHQUAKE_CONDOMINIUM":  [25000, 50000, 75000, 100000],
    "EARTHQUAKE_MOBILEHOME":   [50000, 100000],
    "EARTHQUAKE_RENTERS":      [15000, 25000, 30000, 50000, 70000],
}

_AGE_BUCKETS = [
    (2,  "New"),
    (5,  "3 Years"),
    (11, "6 Years"),
    (21, "15 Years"),
    (33, "25 Years"),
    (56, "40 Years"),
    (float("inf"), "70 Years"),
]


def _age_to_bucket(age: int) -> str:
    for threshold, bucket in _AGE_BUCKETS:
        if age < threshold:
            return bucket
    return "70 Years"


def _year_to_age_bucket(year_built: int) -> str:
    return _age_to_bucket(date.today().year - year_built)



def _nearest_cdi_amount(coverage_type: str, amount: float) -> int:
    valid = _CDI_VALID_AMOUNTS.get(coverage_type.upper(), [])
    if not valid:
        return int(amount)
    return min(valid, key=lambda v: abs(v - amount))


_cdi_service = CDILookupService()


class HomeQuoteRequest(BaseModel):
    zip_code: str = Field(..., description="5-digit California ZIP code")
    coverage_type: str = Field(..., description="HOMEOWNERS | CONDOMINIUM | MOBILEHOME | RENTERS | EARTHQUAKE_*")
    coverage_amount: float = Field(..., description="Dollar amount of coverage, e.g. 375000")
    deductible: int = Field(..., description="Deductible amount: 500, 1000, 2500, or 5000")
    year_built: Optional[int] = Field(None, description="Year the home was built — required for HOMEOWNERS/MOBILEHOME; auto-mapped to CDI age bucket")
    endorsements: Optional[List[str]] = Field(None, description="Endorsement codes to add, e.g. ['ERC_150', 'REPLACEMENT_PERSONAL_PROPERTY']")


class MarketCompany(BaseModel):
    company: str
    annual_premium: int
    monthly_premium: float


class MarketStats(BaseModel):
    count: int
    minimum: int
    maximum: int
    mean: float
    median: float
    percentile_80: float
    percentile_90: float
    percentile_95: float


class HomeQuoteResponse(BaseModel):
    zip_code: str
    county: str
    city: str
    cdi_location: str
    coverage_type: str
    coverage_amount: float
    deductible: int
    age_of_home: Optional[str]
    coverage_package: dict   # coverage_a, coverage_b, coverage_c, coverage_d
    # Factor-based estimate
    base_annual_premium: float
    endorsement_premium: float
    endorsements_applied: List[dict]
    estimated_annual_premium: float
    estimated_monthly_premium: float
    county_risk_tier: str
    factors: dict
    # CDI live market data
    cdi_coverage_amount_used: Optional[int]
    market_stats: Optional[MarketStats]
    market_companies: Optional[List[MarketCompany]]


@router.post("/home-quote", response_model=HomeQuoteResponse, tags=["Insurance Quotes"])
async def home_insurance_quote(request: HomeQuoteRequest):
    """
    Calculate a California home insurance estimate and fetch live CDI market rates.

    - Resolves ZIP code to county and CDI location (no external API needed).
    - Returns a factor-based premium estimate alongside live per-company rates
      from the CDI interactive tool with market statistics.
    """
    # 1. Resolve ZIP → county / CDI location
    zip_info = resolve_zip_info(request.zip_code)
    if not zip_info:
        raise HTTPException(status_code=422, detail=f"ZIP code '{request.zip_code}' is not a recognized California ZIP code.")

    county = zip_info["county"]
    city = zip_info["city"]
    cdi_location = zip_info["cdi_location"]

    # 2. Resolve coverage type, bucket coverage amount, and age bucket
    coverage_type = request.coverage_type.upper()
    coverage_amount = _nearest_cdi_amount(coverage_type, request.coverage_amount)
    age_of_home: Optional[str] = None
    if coverage_type in {"HOMEOWNERS", "MOBILEHOME"}:
        if not request.year_built:
            raise HTTPException(
                status_code=422,
                detail="'year_built' is required for HOMEOWNERS and MOBILEHOME coverage types."
            )
        age_of_home = _year_to_age_bucket(request.year_built)

    # 3. Factor-based premium estimate (with endorsements)
    try:
        quote = calculate_home_insurance(
            coverage_type=coverage_type,
            county=county,
            coverage_amount=coverage_amount,
            deductible=request.deductible,
            age_of_home=age_of_home,
            endorsements=request.endorsements or [],
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    # 4. CDI live market lookup
    market_stats = None
    market_companies = None
    try:
        cdi_result = _cdi_service.lookup(
            location=cdi_location,
            coverage_type=coverage_type,
            coverage_amount=coverage_amount,
            home_age=age_of_home,
        )
        if cdi_result.stats:
            s = cdi_result.stats
            deductible_factor = get_deductible_factor(request.deductible)
            market_stats = MarketStats(
                count=s.count,
                minimum=round(s.minimum * deductible_factor),
                maximum=round(s.maximum * deductible_factor),
                mean=round(s.mean * deductible_factor),
                median=round(s.median * deductible_factor),
                percentile_80=round(s.percentile_80 * deductible_factor),
                percentile_90=round(s.percentile_90 * deductible_factor),
                percentile_95=round(s.percentile_95 * deductible_factor),
            )
            market_companies = [
                MarketCompany(
                    company=c.company,
                    annual_premium=round(c.annual_premium * deductible_factor),
                    monthly_premium=round(c.annual_premium * deductible_factor / 12),
                )
                for c in cdi_result.companies
            ]
    except Exception as e:
        logger.warning(f"CDI live lookup failed for {cdi_location}: {e}")

    return HomeQuoteResponse(
        zip_code=request.zip_code,
        county=county,
        city=city,
        cdi_location=cdi_location,
        coverage_type=coverage_type,
        coverage_amount=coverage_amount,
        deductible=request.deductible,
        age_of_home=age_of_home,
        coverage_package=quote.coverage_package,
        base_annual_premium=quote.base_annual_premium,
        endorsement_premium=quote.endorsement_premium,
        endorsements_applied=quote.endorsements_applied,
        estimated_annual_premium=quote.annual_premium,
        estimated_monthly_premium=quote.monthly_premium,
        county_risk_tier=quote.county_risk_tier,
        factors=quote.breakdown,
        cdi_coverage_amount_used=coverage_amount,
        market_stats=market_stats,
        market_companies=market_companies,
    )


@router.get("/home-endorsements", tags=["Insurance Quotes"])
def home_endorsements_catalog():
    """Returns the full endorsements catalog with codes, descriptions, and pricing."""
    from app.services.calculations.home.home_insurance import _calculator
    _calculator._ensure_loaded()
    return {"endorsements": _calculator._endorsements.reset_index().to_dict(orient="records")}
