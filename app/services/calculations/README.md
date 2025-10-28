# Calculations Services

This folder contains services related to insurance calculations and pricing orchestration.

## Services Overview

### Discount Service
**File:** `discount_service.py`

**Purpose:** Handles discount calculations and lookups for insurance policies.

**Key Features:**
- Discount factor lookups
- Multi-policy discounts
- Safety record discounts
- Loyalty discounts

### Driver Factor Lookup Service
**File:** `driver_factor_lookup_service.py`

**Purpose:** Retrieves driver-specific rating factors from the database.

**Key Features:**
- Driver age factors
- Driving record factors
- License status factors
- Experience factors

### Safety Record Service
**File:** `safety_record_service.py`

**Purpose:** Processes and calculates safety record adjustments.

**Key Features:**
- Violation lookups
- Accident history processing
- Safety record scoring
- Risk assessment

### Pricing Orchestrator
**File:** `pricing_orchestrator.py`

**Purpose:** Main orchestrator that coordinates all calculation services for premium calculation.

**Key Features:**
- Microservices coordination
- Factor aggregation
- Premium calculation
- Error handling and logging

## Usage

```python
from services.calculations import (
    DiscountService,
    DriverFactorLookupService,
    SafetyRecordService,
    PricingOrchestrator
)

# Use individual services
discount_service = DiscountService()
driver_service = DriverFactorLookupService()
safety_service = SafetyRecordService()

# Use main orchestrator
orchestrator = PricingOrchestrator(carrier_config)
result = orchestrator.calculate_premium(rating_input)
```

## Architecture

The calculations services follow a microservices architecture where:

1. **Individual Services** handle specific calculation aspects
2. **Pricing Orchestrator** coordinates all services
3. **Clean Separation** of concerns for maintainability
4. **Modular Design** allows independent testing and updates

## Dependencies

- `models.models` - Data models and input structures
- `services.aggregation_services` - Data aggregation services
- `services.lookup_services` - Data lookup services
