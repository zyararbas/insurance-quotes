# End-to-End Pricing Calculation Documentation

This document details the complete flow of the insurance pricing calculation system, including all required inputs and the order of operations for each service.

## Table of Contents

1. [Required Inputs](#required-inputs)
2. [JSON Sample](#json-sample)
3. [Calculation Flow Overview](#calculation-flow-overview)
4. [Order of Operations](#order-of-operations)
5. [Service Details](#service-details)
6. [Final Premium Formula](#final-premium-formula)

---

## Required Inputs

The pricing calculation requires a `RatingInput` object containing the following information:

### Core Information
- **carrier**: Insurance carrier (e.g., "STATEFARM")
- **state**: State code (e.g., "CA")
- **zip_code**: 5-digit ZIP code for territory factor lookup

### Vehicle Information (`Vehicle` object)
- **year**: Vehicle year (1980-2025)
- **make**: Vehicle make (e.g., "TOYOTA")
- **model**: Vehicle model (e.g., "CAMRY")
- **series**: Optional vehicle series
- **package**: Optional vehicle package
- **style**: Optional vehicle style
- **engine**: Optional engine specification
- **msrp**: Optional MSRP value

### Coverage Information (`Coverages` object)
Each coverage type (BIPD, COLL, COMP, MPC, UM) can have:
- **selected**: Boolean indicating if coverage is selected
- **limits**: Coverage limits (string format, e.g., "15/30/5" for BIPD)
- **deductible**: Deductible amount (integer, e.g., 500)

### Driver Information (`Driver` objects - List)
Each driver must include:
- **driver_id**: Unique identifier for the driver
- **years_licensed**: Years licensed to drive (0-80)
- **safety_record_level**: Optional safety record level (0-30). If not provided, will be calculated from violations
- **percentage_use**: Percentage of vehicle use by this driver (0-100)
- **assigned_driver**: Boolean indicating if driver is assigned to vehicle
- **age**: Optional driver age (16-100)
- **marital_status**: Optional marital status ('S' or 'M')
- **violations**: List of violations/accidents, each containing:
  - **type**: Violation type ('Chargable Accident', 'Minor Moving Voilation', 'Major Violation')
  - **date**: Date of violation
  - **points_added**: Points added for the violation

### Usage Information (`Usage` object)
- **annual_mileage**: Annual mileage (integer, >= 0)
- **type**: Usage type ('Pleasure / Work / School', 'Business', 'Farm')
- **single_automobile**: Boolean indicating if this is the only automobile

### Discounts (`Discounts` object)
- **car_safety_rating**: Optional car safety rating
- **good_driver**: Boolean for good driver discount
- **good_student**: Boolean for good student discount
- **inexperienced_driver_education**: Boolean for driver education discount
- **mature_driver_course**: Boolean for mature driver course discount
- **multi_line**: Optional multi-line discount type
- **student_away_at_school**: Boolean for student away at school discount
- **loyalty_years**: Number of years with carrier (integer)

### Special Factors (`SpecialFactors` object)
- **federal_employee**: Boolean for federal employee discount
- **transportation_network_company**: Boolean for transportation network company factor
- **transportation_of_friends**: Boolean for transportation of friends factor

---

## JSON Sample

### Basic Example (Single Driver, Minimal Data)

```json
{
  "carrier": "STATEFARM",
  "state": "CA",
  "zip_code": "90210",
  "vehicle": {
    "year": 2020,
    "make": "TOYOTA",
    "model": "CAMRY",
    "series": "LE",
    "package": "",
    "style": "4D SEDAN",
    "engine": "2.5L 4-CYL",
    "msrp": 25000
  },
  "coverages": {
    "BIPD": {
      "selected": true,
      "limits": "15/30/5",
      "deductible": null
    },
    "COLL": {
      "selected": true,
      "limits": null,
      "deductible": 500
    },
    "COMP": {
      "selected": true,
      "limits": null,
      "deductible": 500
    },
    "MPC": null,
    "UM": null
  },
  "drivers": [
    {
      "driver_id": "driver1",
      "years_licensed": 10,
      "safety_record_level": null,
      "percentage_use": 100.0,
      "assigned_driver": true,
      "age": 35,
      "marital_status": "M",
      "violations": []
    }
  ],
  "discounts": {
    "car_safety_rating": null,
    "good_driver": true,
    "good_student": false,
    "inexperienced_driver_education": false,
    "mature_driver_course": false,
    "multi_line": "home",
    "student_away_at_school": false,
    "loyalty_years": 5
  },
  "special_factors": {
    "federal_employee": false,
    "transportation_network_company": false,
    "transportation_of_friends": false
  },
  "usage": {
    "annual_mileage": 12000,
    "type": "Pleasure / Work / School",
    "single_automobile": false
  }
}
```

### Comprehensive Example (Multiple Drivers, All Coverages, Violations)

```json
{
  "carrier": "STATEFARM",
  "state": "CA",
  "zip_code": "94102",
  "vehicle": {
    "year": 2022,
    "make": "HONDA",
    "model": "CIVIC",
    "series": "EX",
    "package": "TOURING",
    "style": "4D SEDAN",
    "engine": "1.5L TURBO",
    "msrp": 28000
  },
  "coverages": {
    "BIPD": {
      "selected": true,
      "limits": "100/300/100",
      "deductible": null
    },
    "COLL": {
      "selected": true,
      "limits": null,
      "deductible": 1000
    },
    "COMP": {
      "selected": true,
      "limits": null,
      "deductible": 500
    },
    "MPC": {
      "selected": true,
      "limits": "5000",
      "deductible": null
    },
    "UM": {
      "selected": true,
      "limits": "100/300",
      "deductible": null
    }
  },
  "drivers": [
    {
      "driver_id": "driver1",
      "years_licensed": 15,
      "safety_record_level": null,
      "percentage_use": 70.0,
      "assigned_driver": true,
      "age": 42,
      "marital_status": "M",
      "violations": [
        {
          "type": "Minor Moving Voilation",
          "date": "2023-06-15",
          "points_added": 1
        }
      ]
    },
    {
      "driver_id": "driver2",
      "years_licensed": 8,
      "safety_record_level": 0,
      "percentage_use": 30.0,
      "assigned_driver": true,
      "age": 28,
      "marital_status": "S",
      "violations": []
    }
  ],
  "discounts": {
    "car_safety_rating": "5_STAR",
    "good_driver": true,
    "good_student": false,
    "inexperienced_driver_education": false,
    "mature_driver_course": true,
    "multi_line": "life",
    "student_away_at_school": false,
    "loyalty_years": 10
  },
  "special_factors": {
    "federal_employee": true,
    "transportation_network_company": false,
    "transportation_of_friends": false
  },
  "usage": {
    "annual_mileage": 15000,
    "type": "Business",
    "single_automobile": true
  }
}
```

### Minimal Example (Required Fields Only)

```json
{
  "carrier": "STATEFARM",
  "state": "CA",
  "zip_code": "90001",
  "vehicle": {
    "year": 2018,
    "make": "FORD",
    "model": "F150",
    "series": "",
    "package": "",
    "style": "",
    "engine": ""
  },
  "coverages": {
    "BIPD": {
      "selected": true,
      "limits": "15/30/5"
    },
    "COLL": null,
    "COMP": null,
    "MPC": null,
    "UM": null
  },
  "drivers": [
    {
      "driver_id": "driver1",
      "years_licensed": 5,
      "percentage_use": 100.0,
      "assigned_driver": true
    }
  ],
  "discounts": {},
  "special_factors": {},
  "usage": {
    "annual_mileage": 10000,
    "type": "Pleasure / Work / School",
    "single_automobile": false
  }
}
```

### Example with Safety Record Calculation

```json
{
  "carrier": "STATEFARM",
  "state": "CA",
  "zip_code": "92660",
  "vehicle": {
    "year": 2019,
    "make": "TESLA",
    "model": "MODEL 3",
    "series": "LONG RANGE",
    "package": "",
    "style": "SEDAN",
    "engine": "ELECTRIC"
  },
  "coverages": {
    "BIPD": {
      "selected": true,
      "limits": "250/500/100"
    },
    "COLL": {
      "selected": true,
      "deductible": 500
    },
    "COMP": {
      "selected": true,
      "deductible": 500
    },
    "MPC": null,
    "UM": null
  },
  "drivers": [
    {
      "driver_id": "driver1",
      "years_licensed": 3,
      "safety_record_level": null,
      "percentage_use": 100.0,
      "assigned_driver": true,
      "age": 25,
      "marital_status": "S",
      "violations": [
        {
          "type": "Chargable Accident",
          "date": "2022-03-10",
          "points_added": 3
        },
        {
          "type": "Major Violation",
          "date": "2023-11-20",
          "points_added": 2
        }
      ]
    }
  ],
  "discounts": {
    "good_driver": false,
    "loyalty_years": 2
  },
  "special_factors": {
    "federal_employee": false,
    "transportation_network_company": true,
    "transportation_of_friends": false
  },
  "usage": {
    "annual_mileage": 20000,
    "type": "Pleasure / Work / School",
    "single_automobile": false
  }
}
```

### Notes on JSON Structure

- **Coverage Selection**: Set `selected: true` to include a coverage, or `null` to exclude it
- **BIPD Limits**: Format as "BI/PD" (e.g., "15/30/5" means 15k BI per person, 30k BI per occurrence, 5k PD)
- **UM Limits**: Format as "BI/PD" (e.g., "100/300")
- **MPC Limits**: Integer as string (e.g., "5000" for $5,000)
- **Deductibles**: Integer value (e.g., 250, 500, 1000, 2500)
- **Violations**: If provided, `safety_record_level` will be calculated automatically with time decay
- **Driver Percentage Use**: Must sum to 100% across all drivers
- **Multi-line Discount**: Values can be "home", "life", or null
- **Usage Type**: Must be exactly one of: "Pleasure / Work / School", "Business", or "Farm"

---

## Calculation Flow Overview

The pricing calculation follows this high-level flow:

```
RatingInput
    ↓
PricingOrchestrator
    ↓
CoverageCalculationAggregator
    ↓
[Step 1] BaseRateLookupService
    ↓
[Step 2] DriverAdjustmentAggregator
    ↓
[Step 3] VehicleFactorLookupService
    ↓
[Step 4] CoverageFactorLookupService
    ↓
[Step 5] DiscountService
    ↓
[Step 6] Final Premium Calculation
    ↓
Result (with detailed breakdowns)
```

---

## Order of Operations

### Step 1: Base Rate Calculation
**Service**: `BaseRateLookupService`  
**Method**: `calculate_base_factors(zip_code, coverages)`

**Operations**:
1. Load base rate table (coverage-specific base rates)
2. Load ZIP code territory factor table
3. For each selected coverage:
   - Lookup base rate for coverage type
   - Lookup territory factor for ZIP code and coverage
   - Calculate territorial rate = base_rate × territory_factor

**Output**: 
```python
{
    coverage: {
        'base_rate': float,
        'territory_factor': float,
        'territorial_rate': float
    }
}
```

---

### Step 2: Driver Adjustment Factor Calculation
**Service**: `DriverAdjustmentAggregator`  
**Method**: `calculate_driver_adjustment_factors(drivers, usage, coverages, discounts)`

**Operations**:
For each coverage and each driver:

1. **Base Driver Factor** (`DriverFactorLookupService.get_base_driver_factor`)
   - Lookup base driver factor based on coverage, driver age, and marital status

2. **Years Licensed Factor** (`DriverFactorLookupService.get_years_licensed_factor`)
   - Lookup factor based on years licensed and coverage type

3. **Percentage Use Factor** (`DriverFactorLookupService.get_percentage_use_factor`)
   - Lookup factor based on driver's percentage of vehicle use

4. **Safety Record Factor** (`DriverFactorLookupService.get_safety_record_factor`)
   - If violations exist or safety_record_level not provided:
     - Calculate safety record level using `SafetyRecordService.calculate_safety_record_level`
     - Applies time decay to violation points
   - Lookup safety record factor based on calculated/provided level

5. **Single Automobile Factor** (`DriverFactorLookupService.get_single_automobile_factor`)
   - Lookup factor based on usage.single_automobile flag

6. **Annual Mileage Factor** (`DriverFactorLookupService.get_annual_mileage_factor`)
   - Lookup factor based on annual mileage and coverage type

7. **Usage Type Factor** (`DriverFactorLookupService.get_usage_type_factor`)
   - Lookup factor based on usage type (Pleasure/Business/Farm)

8. **Calculate Combined Driver Factor**
   - Multiply all driver factors: base_factor × years_licensed_factor × percentage_use_factor × safety_record_factor × single_auto_factor × annual_mileage_factor × usage_type_factor

9. **Aggregate Multiple Drivers**
   - If multiple drivers, multiply their combined factors together

**Output**:
```python
{
    coverage: {
        'driver_adjustment_factor': float,
        'base_combined_factor': float,
        'drivers': [
            {
                'driver_id': str,
                'factors': {
                    'base_factor': float,
                    'years_licensed_factor': float,
                    'percentage_use_factor': float,
                    'safety_record_factor': float,
                    'single_auto_factor': float,
                    'annual_mileage_factor': float,
                    'usage_type_factor': float,
                    'driver_combined_factor': float
                }
            }
        ],
        'safety_record_info': dict
    }
}
```

---

### Step 3: Vehicle Factor Calculation
**Service**: `VehicleFactorLookupService`  
**Method**: `calculate_vehicle_factors(vehicle, usage, coverages)`

**Operations**:
1. **Get Vehicle Rating Groups** (`get_vehicle_rating_groups`)
   - Lookup DRG (Driver Rating Group), GRG (Group Rating Group), VSD (Vehicle Symbol Designation), LRG (Liability Rating Group)
   - Uses progressive matching: exact match → fallback match → defaults

2. **Get Model Year Factor** (`get_model_year_factor`)
   - Lookup model year factor based on vehicle year and coverage type

3. **Get LRG Factor** (`get_lrg_factor`) - Only for BIPD coverage
   - Lookup LRG factor based on LRG code and coverage type

4. **Calculate Combined Vehicle Factor**
   - For BIPD: model_year_factor × lrg_factor
   - For other coverages: model_year_factor

**Output**:
```python
{
    coverage: {
        'combined_factor': float,
        'breakdown': {
            'model_year_factor': float,
            'lrg_factor': float (BIPD only)
        }
    }
}
```

---

### Step 4: Coverage Factor Calculation
**Service**: `CoverageFactorLookupService`  
**Method**: `calculate_coverage_factors(coverages, vehicle_rating_groups)`

**Operations**:
For each selected coverage:

1. **BIPD Coverage**:
   - Lookup BI (Bodily Injury) limits factor
   - Lookup PD (Property Damage) limits factor
   - Coverage factor = BI factor × PD factor

2. **COLL Coverage**:
   - Lookup deductible factor based on DRG and deductible amount
   - Coverage factor = deductible factor

3. **COMP Coverage**:
   - Lookup deductible factor based on GRG and deductible amount
   - Coverage factor = deductible factor

4. **MPC Coverage**:
   - Lookup MPC limits factor
   - Coverage factor = limits factor

5. **UM Coverage**:
   - Lookup UM limits factor
   - Coverage factor = limits factor

**Output**:
```python
{
    coverage: {
        'factor': float,
        'breakdown': {
            # Coverage-specific breakdown details
        }
    }
}
```

---

### Step 5: Discount Factor Calculation
**Service**: `DiscountService`  
**Method**: `calculate_discount_factors(discounts, special_factors, coverages)`

**Operations**:
For each selected coverage, calculate the following discount factors:

1. **Loyalty Discount** (`_calculate_loyalty_discount`)
   - Based on loyalty_years and coverage type

2. **Federal Employee Discount** (`_calculate_federal_employee_discount`)
   - Based on special_factors.federal_employee flag and coverage type

3. **Good Driver Discount** (`_calculate_good_driver_discount`)
   - Based on discounts.good_driver flag

4. **Transportation Friends Factor** (`_calculate_transportation_friends_factor`)
   - Based on special_factors.transportation_of_friends flag and coverage type

5. **Transportation Network Factor** (`_calculate_transportation_network_factor`)
   - Based on special_factors.transportation_network_company flag and coverage type

6. **Multi-line Discount** (`_calculate_multi_line_discount`)
   - Based on discounts.multi_line value and coverage type

**Output**:
```python
{
    coverage: {
        'breakdown': {
            'loyalty': float,
            'federal_employee': float,
            'good_driver': float,
            'transportation_friends': float,
            'transportation_network': float,
            'multi_line': float
        },
        'combined_factor': float
    }
}
```

---

### Step 6: Final Premium Calculation
**Service**: `CoverageCalculationAggregator`  
**Method**: `calculate_coverage_premiums(rating_input)`

**Operations**:
For each selected coverage, calculate premium using the following step-by-step formula:

1. **Step 1**: Base Rate
   - `base_rate = base_factors[coverage]['base_rate']`

2. **Step 2**: Base Rate × Territory Factor
   - `territory_factor = base_factors[coverage]['territory_factor']`
   - `step2_total = base_rate × territory_factor`

3. **Step 3**: Step 2 × Coverage Factor
   - `coverage_factor = coverage_factors[coverage]['factor']`
   - `step3_total = step2_total × coverage_factor`

4. **Step 4**: Step 3 × Driver Adjustment Factor (without single auto)
   - Extract driver factors (excluding single_auto_factor)
   - `driver_combined_factor = base_factor × years_licensed_factor × percentage_use_factor × safety_record_factor × annual_mileage_factor × usage_type_factor`
   - `step4_total = step3_total × driver_combined_factor`

5. **Step 5**: Step 4 × Single Auto Factor
   - `single_auto_factor = driver_factors['single_auto_factor']`
   - `step5_total = step4_total × single_auto_factor`

6. **Step 6**: Step 5 × Vehicle Factor (Model Year + LRG)
   - `vehicle_factor = vehicle_factors[coverage]['combined_factor']`
   - `step6_total = step5_total × vehicle_factor`

7. **Step 7**: Step 6 × LRG Factor (BIPD only)
   - If coverage is BIPD:
     - `lrg_factor = vehicle_factors[coverage]['breakdown']['lrg_factor']`
     - `step7_total = step6_total × lrg_factor`
   - Otherwise: `step7_total = step6_total`

8. **Step 8**: Step 7 × Loyalty Discount
   - `loyalty_factor = discount_factors[coverage]['loyalty']`
   - `current_total = step7_total × loyalty_factor`

9. **Step 9**: Step 8 × Federal Employee Discount
   - `federal_employee_factor = discount_factors[coverage]['federal_employee']`
   - `current_total = current_total × federal_employee_factor`

10. **Step 10**: Step 9 × Good Driver Discount
    - `good_driver_factor = discount_factors[coverage]['good_driver']`
    - `current_total = current_total × good_driver_factor`

11. **Step 11**: Step 10 × Transportation Friends Factor
    - `transportation_friends_factor = discount_factors[coverage]['transportation_friends']`
    - `current_total = current_total × transportation_friends_factor`

12. **Step 12**: Step 11 × Transportation Network Factor
    - `transportation_network_factor = discount_factors[coverage]['transportation_network']`
    - `current_total = current_total × transportation_network_factor`

13. **Step 13**: Step 12 × Multi-line Discount
    - `multi_line_factor = discount_factors[coverage]['multi_line']`
    - `coverage_premium = current_total × multi_line_factor`

14. **Calculate Total Premium**
    - Sum all coverage premiums

**Output**:
```python
{
    'input': dict,  # Original RatingInput
    'premiums': {
        coverage: float  # Premium for each coverage
    },
    'total_premium': float,
    'breakdowns': {
        'base_factors': dict,
        'driver_adjustment_factors': dict,
        'vehicle_factors': dict,
        'coverage_factors': dict,
        'vehicle_rating_groups': dict,
        'discount_factors': dict
    },
    'calculation_summary': {
        'formula': str,
        'driver_adjustment_factors': dict,
        'vehicle_factors': dict,
        'coverage_factors': dict,
        'discount_factors': dict
    },
    'metadata': {
        'carrier': str,
        'engine': str
    }
}
```

---

## Final Premium Formula

The complete premium calculation formula for each coverage is:

```
Premium = Base Rate 
        × Territory Factor 
        × Coverage Factor 
        × (Base Driver Factor × Years Licensed Factor × Percentage Use Factor × Safety Record Factor × Annual Mileage Factor × Usage Type Factor)
        × Single Auto Factor
        × Vehicle Factor (Model Year × LRG for BIPD)
        × LRG Factor (BIPD only)
        × Loyalty Discount
        × Federal Employee Discount
        × Good Driver Discount
        × Transportation Friends Factor
        × Transportation Network Factor
        × Multi-line Discount
```

**Total Premium** = Sum of all coverage premiums

---

## Service Initialization

All services must be initialized before use. The initialization process:

1. **PricingOrchestrator.initialize()**
   - Initializes CoverageCalculationAggregator

2. **CoverageCalculationAggregator.initialize()**
   - Initializes BaseRateLookupService
   - Initializes VehicleFactorLookupService
   - Initializes CoverageFactorLookupService
   - Initializes DriverAdjustmentAggregator

3. **DriverAdjustmentAggregator.initialize()**
   - Initializes DriverFactorLookupService
   - Initializes SafetyRecordService

4. **Individual Lookup Services**
   - Load all required data tables from CSV files
   - Cache data in memory for fast lookups

---

## Data Dependencies

The pricing calculation relies on the following data tables (located in `app/services/Data/California/STATEFARM_CA_Insurance__tables/`):

### Base Factors
- `base_factors/base_rates - Sheet1.csv`
- `base_factors/CA_zip_territory_factors - Sheet1.csv`

### Vehicle Factors
- `car_factors/` - Vehicle rating groups and model year factors
- `car_factors/fallback_vehicle_rating_groups - Sheet1.csv`

### Coverage Factors
- `coverage_factors/` - Coverage limits and deductible factors

### Driver Factors
- `driver_factors/` - Driver age, years licensed, safety record, mileage, usage type factors

### Discounts
- `discounts/` - Various discount factor tables

---

## Error Handling

- Missing data defaults to neutral factors (1.0) or default values
- Invalid ZIP codes default to territory factor of 1.0
- Missing vehicle matches use fallback rating groups or defaults
- All errors are logged with appropriate warning/error messages

---

## Example Calculation Flow

```
Input: RatingInput with 1 driver, 1 vehicle, BIPD + COLL coverage
    ↓
Step 1: Base Rate Lookup
    - BIPD base rate: $100
    - COLL base rate: $50
    - Territory factor (90210): 1.2
    - Territorial rates: $120, $60
    ↓
Step 2: Driver Adjustment
    - Base factor: 1.0
    - Years licensed factor: 0.9
    - Safety record factor: 1.0
    - Annual mileage factor: 1.1
    - Combined: 0.99
    ↓
Step 3: Vehicle Factors
    - Model year factor: 0.95
    - LRG factor (BIPD): 1.05
    - Combined (BIPD): 0.9975
    - Combined (COLL): 0.95
    ↓
Step 4: Coverage Factors
    - BIPD (15/30/5): 1.0
    - COLL ($500 deductible): 1.0
    ↓
Step 5: Discounts
    - Good driver: 0.9
    - Multi-line: 0.95
    ↓
Step 6: Final Calculation
    - BIPD: $120 × 1.0 × 0.99 × 0.9975 × 0.9 × 0.95 = $100.89
    - COLL: $60 × 1.0 × 0.99 × 0.95 × 0.9 × 0.95 = $48.20
    - Total: $149.09
```

---

## Notes

- All factors are multiplicative
- The order of operations is critical and must be followed exactly
- Driver factors are aggregated multiplicatively for multiple drivers
- Safety record levels are calculated with time decay if violations are provided
- Vehicle rating groups use progressive matching (exact → fallback → default)
- Discounts are applied after all base calculations
- All monetary values are rounded to 2 decimal places in the final output

