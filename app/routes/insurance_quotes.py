from typing import Any, Dict, Optional, List

from fastapi import APIRouter, Query
from fastapi import HTTPException

from pydantic import BaseModel

from app.models.models import ComprehensiveVehicleSearchRequest, RatingInput  # type: ignore
from app.services.calculations.pricing_orchestrator import PricingOrchestrator
from app.services.lookup_services.vehicle_lookup_service import VehicleLookupService
from app.services.vehicle_search.vehicle_spec_orchestrator import VehicleSpecOrchestrator
from app.routes.adapter_service import AdapterService 
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
    "generic": {
        "carrier": "STATEFARM",
        "state": "CA",
        # ... other config ...
    }
}

vehicleLookupService = VehicleLookupService() 
adapter_service = AdapterService()


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
            result = _calculate_single_rating(rating_input)
            if result:
                results.append(result)
        
        return results

    except Exception as e:
        logger.error(f"An error occurred during payload transformation: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An internal error occurred during transformation: {e}")


def _calculate_single_rating(rating_input: RatingInput) -> Optional[Dict[str, Any]]:
    """
    Helper function to calculate premium for a single rating input.
    Returns the result dict or None if an error occurs (logged).
    """
    try:
        carrier_config = CARRIER_CONFIG.get(rating_input.carrier.lower())

        if not carrier_config:
            logger.warning(f"Carrier '{rating_input.carrier}' not supported.")
            return None

        orchestrator = PricingOrchestrator(carrier_config)
        result = orchestrator.calculate_premium(rating_input)
        return result

    except Exception as e:
        logger.error(f"Error calculating premium for carrier {rating_input.carrier}: {e}", exc_info=True)
        return None
        