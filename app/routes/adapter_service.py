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

from app.routes.min_recommended_max_coverages import COVERAGES

class AdapterService:
    """
    A service class to adapt incoming quote payloads into the internal
    RatingInput model format.
    """
    def _calculate_age(self, dob_str: Optional[str]) -> Optional[int]:
        """Calculates age from a date of birth string (MM/DD/YYYY)."""
        if not dob_str:
            return None
        try:
            dob = datetime.strptime(dob_str, "%m/%d/%Y").date()
            today = date.today()
            return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
        except (ValueError, TypeError):
            return None  # Or handle error appropriately

    def _calculate_years_licensed(self, age: Optional[int], age_licensed_str: Optional[str]) -> int:
        """Calculates the number of years a driver has been licensed."""
        if age is None or not age_licensed_str:
            return 0
        try:
            age_licensed = int(age_licensed_str)
            return max(0, age - age_licensed)
        except (ValueError, TypeError):
            return 0

    def _extract_drivers(self, payload: Dict[str, Any]) -> List[Driver]:
        """Extracts and transforms driver information from the payload."""
        drivers_info = payload.get("additional_info", {}).get("drivers", [])
        if not drivers_info:
            return []

        drivers = []
        for i, driver_data in enumerate(drivers_info):
            years_licensed = driver_data.get("yearsLicensed")
            marital_status_string = driver_data.get("maritalStatus")
            marital_status = 'M' if marital_status_string == "married" else 'S'
            driver = Driver(
                driver_id=f"driver_{i+1}",
                years_licensed=int(years_licensed), 
                marital_status=marital_status,
                violations=[],
            )
            drivers.append(driver)
        return drivers

    def _extract_discounts(self, payload: Dict[str, Any]) -> Discounts:
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

    def _extract_usage(self, payload: Dict[str, Any]) -> Usage:
        """Extracts and transforms usage information from the payload."""
        vehicles = payload.get("additional_info", {}).get("vehicles", [])
        # The source payload does not contain usage details like annual mileage or type.
        # We will use sensible defaults as placeholders until the source provides them.
        usage_info = vehicles[0].get("usage", {})
        annual_mileage = vehicles[0].get("annualMileage")
        return Usage(
            annual_mileage=int(annual_mileage),  # Using a common default value
            type= 'Pleasure / Work / School' if usage_info == "Personal Use " else 'Business' if usage_info == "Business" else 'Farm',
            single_automobile=len(vehicles) == 1,
        )

    def _extract_vehicle(self, vehicle_data: Dict[str, Any]) -> Vehicle:
        """Extracts and transforms a single vehicle's information."""
        vin = vehicle_data.get("vin")
    
        return Vehicle(
            year=vehicle_data.get("year"),
            make=vehicle_data.get("make"),
            model=vehicle_data.get("model"), # Using bodyType as model, as no other field fits
            engine=vehicle_data.get('trim_engine', ''),
            package=vehicle_data.get('package', ''),
            style=vehicle_data.get('style', ''),
            series=vehicle_data.get('series', '')
            # series, style, engine, msrp not in payload
        )

    def _extract_coverages(self, payload: Dict[str, Any]) -> Coverages:
        """Extracts and transforms coverages information from the payload."""
        # This is a simplified mapping. A real implementation would need more complex logic
        # to map from the payload's coverage list to the structured Coverages model.
        policy_coverages = payload.get("policy_details", {}).get("data", {}).get("coverages", [])
        coverage_names = {c.get("coverageName", "").lower() for c in policy_coverages}

        return Coverages(
            BIPD=Coverage(selected=True) if "bodily injury" in coverage_names or "property damage" in coverage_names else None,
            COLL=Coverage(selected=True) if "collision" in coverage_names else None,
            COMP=Coverage(selected=True) if "comprehensive" in coverage_names else None,
            MPC=Coverage(selected=True) if "medical payments" in coverage_names else None,
            UM=Coverage(selected=True) if "uninsured motorists" in coverage_names else None,
        )

    def create_rating_inputs_from_payload(self, payload: Dict[str, Any]) -> List[RatingInput]:
        """
        Takes a complex payload from the quotes endpoint and converts it into a list
        of RatingInput objects, one for each vehicle.
        """
        policy_data = payload.get("policy_details", {}).get("data", {})
        state_info = "CA"
        carrier_info = "generic"
        zip_code_full = "" 
        discounts_info = {}
        if policy_data:
            policy_info = policy_data.get("policy", {})
            carrier_info=policy_info.get("carrier", "generic")
            state_info=policy_info.get("address", {}).get("addressRegion")
            discounts_info = self._extract_discounts(payload)
            zip_code_full = policy_info.get("address", {}).get("postalCode", "")
            additional_vehicles_info = policy_data.get("vehicles", [])
            coverage_info = self._extract_coverages(payload)
        else: 
            zip_code_full = payload.get("additional_info", {}).get("general_questions", {}).get("zip_code", "")
            coverages_choise = payload.get("additional_info", {}).get("general_questions", {}).get("coverageLevel", "min")
            coverage_info = COVERAGES.get(coverages_choise.lower(), {})
        
        additional_vehicles_info = payload.get("additional_info", {}).get("vehicles", [])

        # Extract shared information
        
        zip_code = zip_code_full.split("-")[0] if zip_code_full else ""
        drivers = self._extract_drivers(payload)
      
        usage = self._extract_usage(payload)
      

        # These factors are not in the payload, so we use default values.
        special_factors = SpecialFactors()

        rating_inputs = []
        for vehicle_data in additional_vehicles_info:
            vehicle = self._extract_vehicle(vehicle_data)

            rating_input = RatingInput(
                carrier=carrier_info,
                state=state_info,
                zip_code=zip_code,
                vehicle=vehicle,
                coverages=coverage_info,
                drivers=drivers,
                discounts=discounts_info,
                special_factors=special_factors,
                usage=usage,
            )
            rating_inputs.append(rating_input)

        return rating_inputs