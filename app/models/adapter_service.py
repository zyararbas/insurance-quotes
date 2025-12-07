from datetime import date, datetime
from typing import List, Dict, Any, Optional

from app.models.models import (
    RatingInput,
    Vehicle,
    Coverages,
    Coverage,
    Driver,
    Discounts,
    SpecialFactors,
    Usage,
    Violation,
)


def _extract_drivers(payload: Dict[str, Any]) -> List[Driver]:
    """Extracts and transforms driver information from the payload."""
    drivers_info = payload.get("additional_info", {}).get("drivers", [])
    if not drivers_info:
        return []

    drivers = []
    for i, driver_data in enumerate(drivers_info):
        # Calculate age
        age = None
        if dob_str := driver_data.get("dob"):
            try:
                dob = datetime.strptime(dob_str, "%m/%d/%Y").date()
                today = date.today()
                age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
            except (ValueError, TypeError):
                age = None  # Or handle error appropriately

        # Calculate years_licensed
        years_licensed = 0
        if age is not None and (age_licensed_str := driver_data.get("ageLicensed")):
            try:
                age_licensed = int(age_licensed_str)
                years_licensed = max(0, age - age_licensed)
            except (ValueError, TypeError):
                years_licensed = 0

        driver = Driver(
            driver_id=f"driver_{i+1}",
            years_licensed=years_licensed,
            age=age,
            # Assuming 'S' for Single, 'M' for Married. No data in payload, so defaulting to None.
            marital_status=None,
            # No violation data in payload, so it defaults to an empty list.
            violations=[],
        )
        drivers.append(driver)
    return drivers


def _extract_discounts(payload: Dict[str, Any]) -> Discounts:
    """Extracts and transforms discount information from the payload."""
    policy_discounts = payload.get("policy_details", {}).get("data", {}).get("discounts", [])
    discount_names = {d.get("discountName", "").lower().replace(" ", "_") for d in policy_discounts}

    general_questions = payload.get("additional_info", {}).get("general_questions", {})
    insured_duration = general_questions.get("insuredDuration")
    loyalty_years = 0
    if insured_duration == "6-12 mo":
        loyalty_years = 1 # Example mapping
    # Add more mappings for insuredDuration if necessary

    return Discounts(
        good_driver="good_driver" in discount_names,
        good_student="good_student" in discount_names,
        mature_driver_course="mature_driver" in discount_names,
        inexperienced_driver_education="new_driver" in discount_names,
        loyalty_years=loyalty_years,
        # multi_line and student_away_at_school not present in payload
    )


def _extract_usage(payload: Dict[str, Any]) -> Usage:
    """Extracts and transforms usage information from the payload."""
    vehicles = payload.get("policy_details", {}).get("data", {}).get("vehicles", [])
    return Usage(
        annual_mileage=12000,  # Using a common default value
        type="Pleasure / Work / School",  # Matching the Literal defined in the Usage model
        single_automobile=len(vehicles) == 1,
    )


def _extract_vehicle(vehicle_data: Dict[str, Any], additional_vehicles_info: List[Dict[str, Any]]) -> Vehicle:
    """Extracts and transforms a single vehicle's information."""
    vin = vehicle_data.get("vehicleIdentificationNumber")
    
    # Find additional info for this vehicle by VIN
    additional_info = {}
    for info in additional_vehicles_info:
        if info.get("vehicle", {}).get("vin") == vin:
            # Assuming 'answer' contains the package/trim info
            additional_info['package'] = info.get("vehicle", {}).get("answer", "")
            break

    return Vehicle(
        year=vehicle_data.get("modelDate"),
        make=vehicle_data.get("brand"),
        model=vehicle_data.get("bodyType"), # Using bodyType as model, as no other field fits
        package=additional_info.get('package', ''),
        # series, style, engine, msrp not in payload
    )


def _extract_coverages(payload: Dict[str, Any]) -> Optional[Coverages]:
    """Extracts and transforms coverages information from the payload."""
    # This is a simplified mapping. A real implementation would need more complex logic
    # to map from the payload's coverage list to the structured Coverages model.
    policy_coverages = payload.get("policy_details", {}).get("data", {}).get("coverages", [])
    
    # If no coverages found, return None to trigger defaults
    if not policy_coverages:
        return None
    
    coverage_names = {c.get("coverageName", "").lower() for c in policy_coverages}
    
    # Check if any coverage is actually selected
    has_bipd = "bodily injury" in coverage_names or "property damage" in coverage_names
    has_coll = "collision" in coverage_names
    has_comp = "comprehensive" in coverage_names
    has_mpc = "medical payments" in coverage_names
    has_um = "uninsured motorists" in coverage_names
    
    # If no coverages are found, return None to trigger defaults
    if not (has_bipd or has_coll or has_comp or has_mpc or has_um):
        return None

    return Coverages(
        BIPD=Coverage(selected=True) if has_bipd else None,
        COLL=Coverage(selected=True) if has_coll else None,
        COMP=Coverage(selected=True) if has_comp else None,
        MPC=Coverage(selected=True) if has_mpc else None,
        UM=Coverage(selected=True) if has_um else None,
    )


def create_rating_inputs_from_payload(payload: Dict[str, Any]) -> List[RatingInput]:
    """
    Takes a complex payload from the quotes endpoint and converts it into a list
    of RatingInput objects, one for each vehicle.
    """
    policy_data = payload.get("policy_details", {}).get("data", {})
    if not policy_data:
        return []

    policy_info = policy_data.get("policy", {})
    vehicles_data = policy_data.get("vehicles", [])
    additional_vehicles_info = payload.get("additional_info", {}).get("vehicles", [])

    # Extract shared information
    zip_code_full = policy_info.get("address", {}).get("postalCode", "")
    zip_code = zip_code_full.split("-")[0] if zip_code_full else ""

    drivers = _extract_drivers(payload)
    discounts = _extract_discounts(payload)
    usage = _extract_usage(payload)
    coverages = _extract_coverages(payload)

    # These factors are not in the payload, so we use default values.
    special_factors = SpecialFactors()

    rating_inputs = []
    vehicle_count = len(vehicles_data)  # Total number of vehicles on policy
    
    for vehicle_data in vehicles_data:
        vehicle = _extract_vehicle(vehicle_data, additional_vehicles_info)

        # Default carrier to STATEFARM if not provided
        carrier = policy_info.get("carrier")
        if not carrier:
            carrier = "STATEFARM"

        rating_input = RatingInput(
            carrier=carrier,
            state=policy_info.get("address", {}).get("addressRegion"),
            zip_code=zip_code,
            vehicle=vehicle,
            coverages=coverages,  # Will use defaults if None
            drivers=drivers,
            discounts=discounts,
            special_factors=special_factors,
            usage=usage,
            vehicle_count=vehicle_count,  # Pass total vehicle count for single_automobile calculation
        )
        rating_inputs.append(rating_input)

    return rating_inputs