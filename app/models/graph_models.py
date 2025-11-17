from pydantic import BaseModel, Field
from typing import List, Optional, Any

# A generic base model to allow extra fields from the graph that we don't explicitly define
class GraphBaseModel(BaseModel):
    class Config:
        extra = 'allow'

class GraphPolicy(GraphBaseModel):
    policyNumber: str
    mainAddress: str
    
class GraphDriver(GraphBaseModel):
    givenName: str
    familyName: Optional[str] = None

class GraphVehicle(GraphBaseModel):
    vehicleMake: str
    modelYear: str
    itemNumber: int

class GraphCoverage(GraphBaseModel):
    coverageName: str
    limitPerPerson: Optional[str] = None
    limitPerOccurrence: Optional[str] = None

class GraphVehicleCoverage(GraphBaseModel):
    vehicleItemNumber: int
    deductibleLimit: Optional[str] = None
    coverageStatus: str
    
class GraphDiscount(GraphBaseModel):
    discountName: str

class GraphData(GraphBaseModel):
    policy: GraphPolicy
    drivers: List[GraphDriver]
    vehicles: List[GraphVehicle]
    coverages: List[GraphCoverage]
    vehicleCoverages: List[GraphVehicleCoverage]
    discounts: List[GraphDiscount]

class InsuranceGraphInput(GraphBaseModel):
    """
    The main model for the incoming data from the insurance graph.
    """
    success: bool
    data: GraphData
