from pydantic import BaseModel, Field, conint, constr, model_validator
from typing import List, Optional, Literal, Dict
from datetime import date

class Vehicle(BaseModel):
    year: conint(ge=1980, le=2025)
    make: constr(strip_whitespace=True, to_upper=True)
    model: constr(strip_whitespace=True, to_upper=True)
    series: Optional[constr(strip_whitespace=True, to_upper=True)] = ''
    package: Optional[constr(strip_whitespace=True, to_upper=True)] = ''
    style: Optional[constr(strip_whitespace=True, to_upper=True)] = ''
    engine: Optional[constr(strip_whitespace=True, to_upper=True)] = ''
    msrp: Optional[float] = Field(None, gt=0)

class Violation(BaseModel):
    type: Literal['Chargable Accident', 'Minor Moving Voilation', 'Major Violation']
    date: date
    points_added: int = Field(..., description="Initial points added for this violation")

class Coverage(BaseModel):
    selected: bool = True
    limits: Optional[str] = None
    deductible: Optional[int] = None

class Coverages(BaseModel):
    BIPD: Optional[Coverage] = None
    COLL: Optional[Coverage] = None
    COMP: Optional[Coverage] = None
    MPC: Optional[Coverage] = None
    UM: Optional[Coverage] = None

class Driver(BaseModel):
    id: str = Field(..., alias='driver_id')
    years_licensed: conint(ge=0, le=80)
    # safety_record_level is always calculated from violations - not settable
    percentage_use: Optional[float] = Field(default=100.0, ge=0, le=100)
    assigned_driver: Optional[bool] = Field(default=True)
    age: Optional[conint(ge=16, le=100)] = None
    marital_status: Optional[Literal['S', 'M']] = None
    violations: Optional[List[Violation]] = Field(default_factory=list, description="List of violations and accidents")
    
    @model_validator(mode='before')
    @classmethod
    def ensure_violations_default(cls, data: dict) -> dict:
        """Ensure violations defaults to empty list if None or not provided."""
        if isinstance(data, dict):
            if 'violations' not in data or data.get('violations') is None:
                data['violations'] = []
        return data

class Discounts(BaseModel):
    car_safety_rating: Optional[str] = None
    good_driver: bool = False
    good_student: bool = False
    inexperienced_driver_education: bool = False
    mature_driver_course: bool = False
    multi_line: Optional[str] = None
    student_away_at_school: bool = False
    loyalty_years: int = 0
    
class SpecialFactors(BaseModel):
    federal_employee: bool = False
    transportation_network_company: bool = False
    transportation_of_friends: bool = False

class Usage(BaseModel):
    annual_mileage: conint(ge=0)
    type: Optional[Literal['Pleasure / Work / School', 'Business', 'Farm']] = Field(default='Pleasure / Work / School')
    single_automobile: Optional[bool] = None  # Will be calculated from vehicle_count if not provided

class ComprehensiveVehicleSearchRequest(BaseModel):
    vin: Optional[str] = None
    make: Optional[str] = None
    model: Optional[str] = None
    year: Optional[int] = None
    additional_info: Optional[str] = None
    conversation_history: Optional[List[Dict[str, str]]] = None

def get_default_coverages() -> Coverages:
    """
    Returns default State Farm coverage configuration:
    - BIPD: 100/300 limits (BI: 100K/300K, PD: 100K)
    - UM: 100/300 limits
    - COLL: $500 deductible
    - COMP: $500 deductible
    - MPC: $5000 limits
    """
    return Coverages(
        BIPD=Coverage(selected=True, limits="100/300"),
        UM=Coverage(selected=True, limits="100/300"),
        COLL=Coverage(selected=True, deductible=500),
        COMP=Coverage(selected=True, deductible=500),
        MPC=Coverage(selected=True, limits="5000")
    )


def get_default_bipd_coverage() -> Coverage:
    """
    Returns default BIPD coverage (Bodily Injury/Property Damage).
    BIPD is required, so we always provide defaults if not specified.
    """
    return Coverage(selected=True, limits="100/300")


class RatingInput(BaseModel):
    carrier: constr(strip_whitespace=True, to_upper=True)
    state: Literal['CA']
    zip_code: constr(pattern=r'^\d{5}$')
    vehicle: Vehicle
    coverages: Optional[Coverages] = None
    drivers: List[Driver] = Field(..., min_items=1)
    discounts: Discounts
    special_factors: SpecialFactors
    usage: Usage
    vehicle_count: Optional[int] = Field(default=1, ge=1, description="Total number of vehicles on policy. Used to calculate single_automobile.")

    @model_validator(mode='before')
    @classmethod
    def set_default_coverages(cls, data: dict) -> dict:
        """
        Apply default coverages based on what's provided:
        - If entire coverages object is None/missing → apply all defaults
        - If coverages object exists as dict:
          - BIPD None → apply default for BIPD (required)
          - Other coverages None → respect as declined (optional)
        """
        if isinstance(data, dict):
            coverages_data = data.get('coverages')
            
            # If entire coverages object is None or missing, apply all defaults
            if coverages_data is None:
                data['coverages'] = get_default_coverages()
            elif isinstance(coverages_data, dict):
                # Coverages object exists as dict - only apply defaults for BIPD if it's None
                # Other None values are respected as declined coverage
                if coverages_data.get('BIPD') is None:
                    # Set BIPD default directly as dict
                    coverages_data['BIPD'] = {'selected': True, 'limits': '100/300', 'deductible': None}
                    data['coverages'] = coverages_data
        return data
    
    @model_validator(mode='after')
    def ensure_bipd_default_and_calculate_single_auto(self) -> 'RatingInput':
        """
        After validation:
        1. Ensure BIPD has a default if it's None
        2. Calculate single_automobile from vehicle_count if not provided
        """
        # Ensure BIPD default
        if self.coverages is None:
            self.coverages = get_default_coverages()
        elif self.coverages.BIPD is None:
            self.coverages.BIPD = get_default_bipd_coverage()
        
        # Calculate single_automobile from vehicle_count if not provided
        if self.usage.single_automobile is None:
            self.usage.single_automobile = (self.vehicle_count == 1)
        
        return self

    class Config:
        allow_population_by_field_name = True
        anystr_strip_whitespace = True
