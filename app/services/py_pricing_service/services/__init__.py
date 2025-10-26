"""
Services Package

This package contains all microservices organized by functionality:

- calculations/ - Insurance calculation services
- lookup_services/ - Data lookup services  
- vehicle_search/ - Vehicle search and AI services
- aggregation_services/ - Data aggregation services
"""

# Import from subpackages
from .calculations import (
    DiscountService,
    DriverFactorLookupService,
    SafetyRecordService,
    PricingOrchestrator
)

from .vehicle_search import (
    VehicleSearchService,
    AIAssistantService,
    VehicleSpecOrchestrator
)

__all__ = [
    # Calculations services
    'DiscountService',
    'DriverFactorLookupService', 
    'SafetyRecordService',
    'PricingOrchestrator',
    
    # Vehicle search services
    'VehicleSearchService',
    'AIAssistantService',
    'VehicleSpecOrchestrator'
]
