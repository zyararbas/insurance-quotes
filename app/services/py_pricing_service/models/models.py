from pydantic import BaseModel, Field, conint, constr
from typing import List, Optional, Literal
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
    safety_record_level: Optional[conint(ge=0, le=30)] = None  # Made optional since we'll calculate it
    percentage_use: float = Field(100.0, ge=0, le=100)
    assigned_driver: bool = True
    age: Optional[conint(ge=16, le=100)] = None
    marital_status: Optional[Literal['S', 'M']] = None
    violations: List[Violation] = Field(default_factory=list, description="List of violations and accidents")

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
    type: Literal['Pleasure / Work / School', 'Business', 'Farm'] = 'Pleasure / Work / School'
    single_automobile: bool = False

class RatingInput(BaseModel):
    carrier: constr(strip_whitespace=True, to_upper=True)
    state: Literal['CA']
    zip_code: constr(pattern=r'^\d{5}$')
    vehicle: Vehicle
    coverages: Coverages
    drivers: List[Driver] = Field(..., min_items=1)
    discounts: Discounts
    special_factors: SpecialFactors
    usage: Usage

    class Config:
        allow_population_by_field_name = True
        anystr_strip_whitespace = True
