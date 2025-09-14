# Microservices Architecture for Coverage Compass AI

## Overview

This document describes the new microservices architecture that has been implemented to replace the monolithic pricing orchestrator. The new architecture provides better isolation, maintainability, and debugging capabilities.

## Architecture Components

### 1. Individual Lookup Services (`services/lookup_services/`)

These services handle individual factor lookups and can be tested independently:

#### BaseRateLookupService
- **Purpose**: Handles territory factors and base rates for each coverage type
- **Key Methods**:
  - `get_territory_factor(zip_code)`: Gets territory factor for a specific zip code
  - `get_base_rate(coverage, territory)`: Gets base rate for a specific coverage and territory
  - `calculate_base_factors(zip_code, coverages)`: Calculates territory-adjusted base rates

#### DriverFactorLookupService
- **Purpose**: Handles all driver-related factor calculations
- **Key Methods**:
  - `get_base_driver_factor(coverage, driver)`: Base driver factor from rating tables
  - `get_years_licensed_factor(coverage, driver)`: Years licensed adjustment factor
  - `get_percentage_use_factor(coverage, driver)`: Percentage use by driver factor
  - `get_safety_record_factor(coverage, safety_level)`: Safety record factor
  - `get_single_automobile_factor(coverage, usage)`: Single automobile factor
  - `get_annual_mileage_factor(coverage, usage)`: Annual mileage factor
  - `get_usage_type_factor(coverage, usage)`: Usage type factor

#### VehicleFactorLookupService
- **Purpose**: Handles vehicle rating groups, model year factors, and LRG factors
- **Key Methods**:
  - `get_vehicle_rating_groups(vehicle)`: Gets DRG, GRG, LRG for a vehicle
  - `get_model_year_factor(coverage, year)`: Model year factor for a coverage and year
  - `get_lrg_factor(coverage, lrg_code)`: LRG factor for a coverage and LRG code
  - `calculate_vehicle_factors(vehicle, usage, coverages)`: Calculates all vehicle factors

#### DiscountLookupService
- **Purpose**: Handles all discount-related factor calculations
- **Key Methods**:
  - `get_good_driver_discount(coverage)`: Good driver discount factor
  - `get_good_student_discount(coverage)`: Good student discount factor
  - `get_inexperienced_driver_discount(coverage)`: Inexperienced driver discount factor
  - `get_mature_driver_discount(coverage)`: Mature driver discount factor
  - `get_student_away_discount(coverage)`: Student away discount factor
  - `get_multi_line_discount(coverage, multi_line_type)`: Multi-line discount factor
  - `get_loyalty_discount(coverage, loyalty_years)`: Loyalty discount factor
  - `get_car_safety_discount(coverage, safety_rating)`: Car safety discount factor
  - `calculate_combined_discount_factor(coverage, discounts)`: Combined discount factor

#### CoverageFactorLookupService
- **Purpose**: Handles coverage limits, deductibles, and their associated factors
- **Key Methods**:
  - `get_bi_factor(coverage, limit)`: BIPD factor for a specific limit
  - `get_pd_factor(coverage, limit)`: PD factor for a specific limit
  - `get_um_factor(coverage, limit)`: UM factor for a specific limit
  - `get_mpc_factor(coverage, limit)`: MPC factor for a specific limit
  - `get_collision_factor(coverage, deductible, drg)`: Collision factor for deductible and DRG
  - `get_comprehensive_factor(coverage, deductible, grg)`: Comprehensive factor for deductible and GRG
  - `calculate_coverage_factors(coverages, vehicle_rating_groups)`: Calculates all coverage factors

### 2. Aggregation Services (`services/aggregation_services/`)

These services combine multiple lookup results for complex calculations:

#### DriverAdjustmentAggregator
- **Purpose**: Aggregates all driver-related factors to calculate the Driver Adjustment Factor
- **Key Methods**:
  - `calculate_driver_adjustment_factors(drivers, usage, coverages, discounts)`: Main aggregation method
  - `get_driver_adjustment_factor_only(coverage, drivers, usage, discounts)`: Gets just the final factor

#### CoverageCalculationAggregator
- **Purpose**: Aggregates all factors to calculate the final coverage premiums
- **Key Methods**:
  - `calculate_coverage_premiums(rating_input)`: Main calculation method
  - `get_coverage_breakdown(coverage, rating_input)`: Detailed breakdown for a specific coverage

### 3. Main Orchestrator

#### NewPricingOrchestrator
- **Purpose**: Coordinates all microservices for the complete premium calculation
- **Key Methods**:
  - `calculate_premium(rating_input)`: Main premium calculation method
  - `get_driver_adjustment_factors(rating_input)`: Gets driver adjustment factors only
  - `get_coverage_breakdown(coverage, rating_input)`: Gets coverage breakdown
  - `get_individual_factors(rating_input)`: Gets all individual factors without aggregation

## API Endpoints

### New Testing Endpoints

#### `/test-driver-adjustment/` (POST)
- **Purpose**: Tests the Driver Adjustment Aggregation Service in isolation
- **Returns**: Only the driver adjustment factors without other calculations
- **Use Case**: Debugging driver factor calculations

#### `/test-coverage-breakdown/` (POST)
- **Purpose**: Tests the Coverage Calculation Aggregation Service for a specific coverage
- **Parameters**: `coverage` (query), `rating_input` (body)
- **Returns**: Detailed breakdown for debugging
- **Use Case**: Debugging coverage calculations

#### `/test-individual-factors/` (POST)
- **Purpose**: Tests all individual lookup services without aggregation
- **Returns**: Raw factors for debugging and analysis
- **Use Case**: Understanding individual lookup values

## Benefits of the New Architecture

### 1. **Isolation**
- Each service can be tested independently
- Issues can be isolated to specific services
- Easier to debug specific factor calculations

### 2. **Maintainability**
- Clear separation of concerns
- Each service has a single responsibility
- Easier to modify individual components

### 3. **Debugging**
- Can test individual services in isolation
- Clear data flow between services
- Detailed logging at each step

### 4. **Scalability**
- Services can be optimized independently
- Easier to add new factor types
- Better performance monitoring

## Usage Examples

### Testing Driver Adjustment Only
```python
# Test just the driver adjustment calculation
response = requests.post(
    "http://localhost:8000/test-driver-adjustment/",
    json=rating_input_data
)
driver_factors = response.json()
```

### Testing Coverage Breakdown
```python
# Test coverage calculation for a specific coverage
response = requests.post(
    "http://localhost:8000/test-coverage-breakdown/?coverage=BIPD",
    json=rating_input_data
)
coverage_breakdown = response.json()
```

### Testing Individual Factors
```python
# Test all individual lookup services
response = requests.post(
    "http://localhost:8000/test-individual-factors/",
    json=rating_input_data
)
individual_factors = response.json()
```

## Migration from Old Architecture

The old `PricingOrchestrator` has been replaced with `NewPricingOrchestrator`. The main calculation endpoint (`/calculate-premium/`) now uses the new microservices architecture, but maintains the same API contract for backward compatibility.

## Testing Strategy

1. **Unit Tests**: Test each lookup service independently
2. **Integration Tests**: Test aggregation services with mock lookup services
3. **End-to-End Tests**: Test the complete flow through the new orchestrator
4. **Comparison Tests**: Compare results with the old orchestrator to ensure accuracy

## Future Enhancements

1. **Caching**: Add caching to frequently accessed lookup services
2. **Async Processing**: Make lookup services asynchronous for better performance
3. **Validation**: Add input validation to each service
4. **Metrics**: Add performance metrics and monitoring
5. **Configuration**: Make factor tables configurable per carrier
