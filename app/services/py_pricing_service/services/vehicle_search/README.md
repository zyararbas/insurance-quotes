# Vehicle Search Services

This package contains the 4 core vehicle services for handling all vehicle specification use cases with built-in deduplication and conflict resolution.

## Architecture

```
vehicle_search/
├── __init__.py                          # Package initialization
├── ai_assistant_service.py              # AI service with built-in deduplication
├── vehicle_search_service.py            # Service 1: Vehicle Search
├── vin_lookup_service.py                # VIN Lookup Service
├── vehicle_spec_orchestrator.py         # Service 4: End-to-End Orchestrator
└── README.md                            # This documentation
```

## Services Overview

### Service 1: Vehicle Search Service
**File:** `vehicle_search_service.py`

**Purpose:** Unified vehicle search with built-in VIN lookup support

**Key Features:**
- **Built-in VIN lookup** - Automatically decodes VINs and extracts search criteria
- **Unified interface** - Handles both VIN-based and manual searches
- **Hybrid searches** - VIN provides base criteria, manual overrides specific fields
- **Automatic fallback** - If VIN lookup fails, continues with manual criteria

**Key Methods:**
- `search_vehicles()` - Unified search supporting VIN and manual criteria
- `get_available_options()` - Get available options with VIN support
- `search_by_vin_only()` - Simplified VIN-only searches
- `search_by_criteria_only()` - Simplified criteria-only searches

**Usage:**
```python
from services.vehicle_search import VehicleSearchService

search_service = VehicleSearchService()

# VIN-based search
result = search_service.search_vehicles(vin="1HGBH41JXMN109186")

# Manual search
result = search_service.search_vehicles(make="BMW", model="X3", year=2020)

# Hybrid search (VIN + manual overrides)
result = search_service.search_vehicles(
    vin="1HGBH41JXMN109186",
    package="CONVENIENCE"  # Override package from VIN
)
```

### Service 2: AI Assistant Service
**File:** `ai_assistant_service.py`

**Purpose:** AI-powered vehicle matching with built-in deduplication and conflict resolution

**Key Features:**
- **Automatic deduplication** of vehicle specifications
- **Business rule conflict resolution**:
  - GRG/DRG/LRG: Use median values
  - VSD: Use most common value (mode)
- **Confidence scoring** based on difference magnitude
- **Audit logging** of all conflict resolutions
- **Complete AI functionality** (OpenAI integration, prompt creation, response parsing)

**Usage:**
```python
from services.vehicle_search import AIAssistantService

ai_service = AIAssistantService(provider="openai")
result = ai_service.interpret_vehicle_results(vin_data, search_results, additional_info)
```

### Service 3: Specific Vehicle Spec Lookup Service
**File:** `services/lookup_services/vehicle_lookup_service.py` (existing)

**Purpose:** Retrieve exact vehicle specifications by precise criteria

**Key Methods:**
- `search_vehicles()` - Get all matching vehicle specifications
- `get_years()`, `get_makes()`, `get_models()`, `get_series()`, `get_packages()`, `get_styles()`

**Usage:**
```python
from services.lookup_services.vehicle_lookup_service import VehicleLookupService

lookup_service = VehicleLookupService()
results = lookup_service.search_vehicles(make="BMW", model="X3", year=2020)
```

### Service 4: Vehicle Spec Orchestrator
**File:** `vehicle_spec_orchestrator.py`

**Purpose:** Lightweight orchestrator that coordinates all vehicle services

**Key Methods:**
- `process_vehicle_request()` - Main entry point for all vehicle requests
- `get_vehicle_spec_by_criteria()` - Direct access to specific vehicle specs

**Usage:**
```python
from services.vehicle_search import VehicleSpecOrchestrator

orchestrator = VehicleSpecOrchestrator()

# VIN-based request
result = orchestrator.process_vehicle_request(
    vin="1HGBH41JXMN109186",
    additional_info="Package: Convenience"
)

# Manual request
result = orchestrator.process_vehicle_request(
    make="BMW", model="X3", year=2020,
    additional_info="Package: Convenience, AWD"
)
```

## Business Rules for Conflict Resolution

### Deduplication Rules
- **GRG (Garage Rating Group)**: Use median value
- **DRG (Driver Rating Group)**: Use median value  
- **VSD (Vehicle Safety Data)**: Use most common value (mode)
- **LRG (Loss Rating Group)**: Use median value

### Confidence Levels
- **High (≤2 difference)**: Use resolved value with high confidence
- **Medium (3-5 difference)**: Use resolved value with warning
- **Low (6-10 difference)**: Flag for manual review
- **Exclude (>10 difference)**: Exclude from AI model, investigate data source

## Data Flow

```
VIN/Manual Input → Vehicle Search → AI Assistant → Final Vehicle Spec
     ↓                    ↓              ↓
VIN Lookup Service → Search Results → Deduplication → AI Model → Result
```

## Key Benefits

1. **Clean Architecture**: 4 focused services with clear responsibilities
2. **Built-in Deduplication**: Automatic conflict resolution before AI processing
3. **Business Rule Compliance**: Consistent conflict resolution across all services
4. **Audit Trail**: Complete logging of conflict resolutions
5. **Confidence Scoring**: Track uncertainty levels for better decision making
6. **Lightweight Orchestration**: Simple coordination without unnecessary complexity

## Integration

### Import Services
```python
from services.vehicle_search import (
    VehicleSearchService,
    AIAssistantService,
    VehicleSpecOrchestrator
)
```

### Simple End-to-End Usage
```python
# Use the orchestrator for complete vehicle specification requests
orchestrator = VehicleSpecOrchestrator()

# VIN-based request
result = orchestrator.process_vehicle_request(
    vin="1HGBH41JXMN109186",
    additional_info="Package: Convenience"
)

# Manual request
result = orchestrator.process_vehicle_request(
    make="BMW", model="X3", year=2020,
    additional_info="Package: Convenience, AWD"
)
```

### Individual Service Usage
```python
# Service 1: Vehicle Search (with built-in VIN lookup)
search_service = VehicleSearchService()
result = search_service.search_vehicles(vin="1HGBH41JXMN109186")

# Service 2: AI Assistant with deduplication
ai_service = AIAssistantService(provider="openai")
result = ai_service.interpret_vehicle_results(vin_data, search_results, additional_info)
```

## Files Structure

### Files Created:
- `vehicle_search_service.py` - Unified search with built-in VIN lookup
- `ai_assistant_service.py` - AI service with built-in deduplication
- `vehicle_spec_orchestrator.py` - Lightweight orchestrator
- `README.md` - This documentation

### Dependencies:
- `services.lookup_services.vehicle_lookup_service` - Vehicle lookup service

## Conflict Resolution Process

### 1. Automatic Deduplication
When multiple vehicles have identical specifications but different ratings:
- **Group by specification** (excluding ratings)
- **Identify conflicts** in rating fields (GRG, DRG, VSD, LRG)
- **Apply business rules** to resolve conflicts

### 2. Business Rule Application
- **GRG/DRG/LRG conflicts**: Use median value (most conservative)
- **VSD conflicts**: Use most common value (mode)
- **Calculate confidence** based on difference magnitude
- **Log all resolutions** for audit trail

### 3. AI Processing
- **Send deduplicated results** to AI model
- **AI determines best match** from clean data
- **Return final vehicle specification** or questions for clarification

## Monitoring and Analytics

### Conflict Statistics
The AI service automatically tracks:
- **Total vehicles** processed
- **Unique specifications** found
- **Conflict groups** identified
- **Rating conflicts** by field (GRG, DRG, VSD, LRG)
- **Confidence levels** of resolutions

### Audit Logging
All conflict resolutions are logged with:
- **Field name** and conflicting values
- **Resolved value** and resolution rule
- **Confidence level** and difference magnitude
- **Vehicle count** in conflict group

## Next Steps

1. **Test the AI service** with your conflict data
2. **Integrate the orchestrator** into your main application
3. **Monitor conflict statistics** to track data quality improvements
4. **Adjust business rules** based on your specific requirements

## Example: Complete Workflow

```python
from services.vehicle_search import VehicleSpecOrchestrator

# Initialize orchestrator
orchestrator = VehicleSpecOrchestrator()

# Process vehicle request (handles all 4 services internally)
result = orchestrator.process_vehicle_request(
    make="BMW",
    model="X3", 
    year=2020,
    additional_info="Package: Convenience, AWD"
)

# Result contains:
# - VIN data (if VIN provided)
# - Search criteria used
# - Total matches found
# - AI interpretation result
# - Deduplication statistics (if conflicts resolved)
# - Final vehicle specification or questions for clarification
```

The vehicle search services provide a complete, clean solution for handling all vehicle specification use cases with built-in deduplication and conflict resolution.