# Insurance Rating Tables Documentation

This directory contains comprehensive insurance rating tables used for auto insurance premium calculations. The data is organized into five main categories that work together to determine insurance rates based on various risk factors.

## Directory Structure

```
Insurance__tables/
├── base_factors/           # Core insurance rates and territory factors
├── car_factors/           # Vehicle-related rating factors
├── coverage_factors/      # Coverage limits and deductible factors
├── discounts/            # Various discount factors
├── driver_factors/       # Driver-related rating factors
└── README.md            # This documentation file
```

## Coverage Types

Throughout all tables, these coverage abbreviations are used:
- **BIPD**: Bodily Injury & Property Damage Liability
- **COLL**: Collision Coverage
- **COMP**: Comprehensive Coverage
- **MPC**: Medical Payments Coverage
- **U/UM**: Uninsured/Underinsured Motorist Coverage

## Data Categories

### 1. Base Factors (`base_factors/`)

#### `base_rates - Sheet1.csv`
Core base rates for each coverage type.
- **Structure**: coverage, base_rate
- **Coverage Types**: BIPD ($841.98), COLL ($869.39), COMP ($50.00), MPC ($91.88), U ($86.20), Car Rental ($20.52)
- **Usage**: Starting point for all premium calculations

#### `CA_zip_territory_factors - Sheet1.csv`
Territory-based adjustment factors by California ZIP code.
- **Structure**: zip, bipd_factor, coll_factor, comp_factor, mpc_factor, um_factor, ers_factor
- **Coverage**: 1,865 ZIP codes (90001-96162)
- **Usage**: Apply territorial risk adjustments based on policyholder location

### 2. Car Factors (`car_factors/`)

#### `annual_mileage_factor - Sheet1.csv`
Mileage-based rating factors.
- **Structure**: Annual Mileage, Lower Bound, BIPD, COLL, COMP, MPC, U
- **Ranges**: 28 mileage bands from 0-1,449 to 33,950+ miles annually
- **Usage**: Adjust rates based on expected annual vehicle usage

#### `auto_usage_type - Sheet1.csv`
Vehicle usage classification factors.
- **Structure**: automobile_use, usage_type_code, bipd_factor, coll_factor, comp_factor, mpc_factor, um_factor
- **Types**: 
  - Farm (code 3): Reduced rates (0.848-1.056)
  - Pleasure/Work/School (code 1): Base rates (1.0)
  - Business (code 2): Mixed rates (0.917-1.257)

#### `fallback_vehicle_rating_groups - Sheet1.csv`
MSRP-based vehicle rating when specific vehicle data unavailable.
- **Structure**: MSRP_range, MSRP_min, MSRP_max, GRG, DRG, VSD, LRG
- **Ranges**: 30 MSRP brackets from $0-2,200 to $70,001-75,000
- **Usage**: Backup rating method for vehicles not in main database

#### `model_year_factors - Sheet1.csv`
Age-based vehicle depreciation factors.
- **Structure**: min_year, bipd_factor, coll_factor, comp_factor, mpc_factor
- **Range**: Model years 1999-2022
- **Base Year**: 2018 (all factors = 1.0)
- **Usage**: Adjust collision/comprehensive rates based on vehicle age

#### `single_auto_factor - Sheet1.csv`
Single vehicle policy surcharge factors.
- **Structure**: coverage, single_automobile_factor
- **Factors**: BIPD (1.259), COLL (1.255), COMP (1.081), MPC (1.263), U (1.319)
- **Usage**: Apply when policy covers only one vehicle

#### `vehicle_ratings_groups - Sheet1.csv`
Comprehensive vehicle database with rating assignments.
- **Structure**: year, make, model, series, package, style, engine, code, grg, drg, vsd, lrg
- **Size**: 26,100+ vehicle records
- **Usage**: Primary vehicle rating lookup table

### 3. Coverage Factors (`coverage_factors/`)

#### `bi_limits - Sheet1.csv`
Bodily Injury liability limit factors.
- **Structure**: bi_limits, factor
- **Limits**: 22 combinations from 30/60 to 1000/1000
- **Range**: Factors 1.03 to 1.61

#### `drg_deductible_factors - Sheet1.csv`
Collision deductible factors by Damage Rating Group.
- **Structure**: DRG (1-35), FULL, 50, 100, 200, 250, 500, 1000, 1000W/20%, 2000
- **Usage**: Apply collision deductible credits based on vehicle's DRG

#### `federal_employee - Sheet1.csv`
Federal employee discount eligibility.
- **Structure**: eligible, factor
- **Discount**: 30% reduction (0.7 factor) for federal employees

#### `grg_deductible - Sheet1.csv`
Comprehensive deductible factors by Glass Rating Group.
- **Structure**: GRG (1-35), deductible options (50-2000)
- **Usage**: Apply comprehensive deductible credits based on vehicle's GRG

#### `lrg_code_factors - Sheet1.csv`
Loss Rating Group adjustment factors.
- **Structure**: lrg, factor
- **Range**: 9 LRG codes with factors 0.75 to 1.18

#### `mpc_coverage_limits - Sheet1.csv`
Medical Payments Coverage limit factors.
- **Structure**: limit, factor
- **Limits**: $500 to $100,000 with factors 0.34 to 3.78

#### `pd_limits - Sheet1.csv`
Property Damage liability limit factors.
- **Structure**: limit, factor
- **Limits**: $15K to $1,000K with factors -0.03 to 0.22

#### `rental_car_limits - Sheet1.csv`
Rental car coverage limit factors.
- **Structure**: limits, factor
- **Options**: Various daily/total limit combinations

#### `transporation_network_company - Sheet1.csv`
Transportation Network Company (TNC) coverage factors.
- **Structure**: Coverage, Factor
- **Usage**: Apply when vehicle used for rideshare services

#### `transportation of friends_or_occupation - Sheet1.csv`
Occasional transportation surcharge.
- **Structure**: eligible, factor
- **Surcharge**: 20% increase (1.2 factor) when applicable

#### `um_limits - Sheet1.csv`
Uninsured Motorist coverage limit factors.
- **Structure**: limits, factor
- **Range**: Factors 1.31 to 5.0 for various limit combinations

### 4. Discounts (`discounts/`)

#### `car_safety_rating_discount - Sheet1.csv`
Vehicle safety rating discounts.
- **Structure**: Safety Code, discount
- **Codes**: A (39%) to E (0%) based on NHTSA/IIHS ratings

#### `good_driver_discount - Sheet1.csv`
Good driving record discount.
- **Structure**: eligible, factor
- **Discount**: 20% reduction (0.2 factor) for qualifying drivers

#### `good_student_discount - Sheet1.csv`
Student academic achievement discount.
- **Structure**: eligible, discount
- **Discount**: 20% for qualifying students

#### `inexperienced_driver_safety_education_discount - Sheet1.csv`
Safety course completion discount for new drivers.
- **Structure**: coverage, discount_factor
- **Range**: Coverage-specific factors from 0.819 to 0.981

#### `loyalty_discount_by_year_coverage - Sheet1.csv`
Tenure-based loyalty discounts.
- **Structure**: tenure_years, bipd_factor, coll_factor, comp_factor, mpc_factor, um_factor
- **Tiers**: 3-6 years with increasing discounts

#### `mature_driver_improvement_course_discount - Sheet1.csv`
Mature driver course completion discount.
- **Structure**: eligible, factor
- **Discount**: 2% reduction (0.02 factor)

#### `mulitple_line_discount - Sheet1.csv`
Multi-policy bundling discounts.
- **Structure**: additional_policies, discount
- **Range**: 4% to 28% based on policy combination

#### `student_away_at_school_discount - Sheet1.csv`
Distant school attendance discount.
- **Structure**: coverage, discount_factor
- **Range**: Coverage-specific factors from 0.901 to 0.981

### 5. Driver Factors (`driver_factors/`)

#### `driving_safety_record_rating_plan - Sheet1.csv`
Comprehensive driving record rating system.
- **Structure**: rate_level, bipd_factor, coll_factor, comp_factor, mpc_factor, um_factor
- **Levels**: 31 levels (0-30) with level 8 as base (1.0)
- **Range**: Factors from 0.506 to 4.899 based on violations/accidents

#### `percentage_use_by_driver - Sheet1.csv`
Driver assignment and usage factors.
- **Structure**: Key, BIPD, COLL, COMP, MPC, U
- **Scenarios**: Based on driver assignment and percentage of use

#### `safety_driver_record_score - Sheet1.csv`
Violation point assignment system.
- **Structure**: violation_type, points_added, decay_period, decay_amounts
- **Types**: 
  - Chargeable Accident: 6 points, 6-year decay
  - Minor Moving Violation: 5 points, 5-year decay
  - Major Violation: 14 points, 7-year decay

#### `years_liscensed_key - Sheet1.csv`
Driving experience adjustment factors.
- **Structure**: Coverage, Assigned Driver, Years Licensed, Years Licensed Key, Lookup Key, Factor
- **Usage**: Apply experience-based adjustments for new vs. experienced drivers

## Data Usage Guidelines

### Rating Calculation Flow
1. Start with **base rates** from `base_factors/base_rates`
2. Apply **territorial factors** from `base_factors/CA_zip_territory_factors`
3. Apply **vehicle factors** from `car_factors/` tables
4. Apply **coverage factors** from `coverage_factors/` tables
5. Apply **driver factors** from `driver_factors/` tables
6. Apply applicable **discounts** from `discounts/` tables

### Key Relationships
- **Vehicle Rating**: Use `vehicle_ratings_groups` primarily, fall back to `fallback_vehicle_rating_groups` if vehicle not found
- **Deductibles**: GRG determines comprehensive deductible factors, DRG determines collision deductible factors
- **Driver Rating**: Combine `driving_safety_record_rating_plan` with `years_licensed_key` and `percentage_use_by_driver`

### Data Maintenance Notes
- All factor tables use multiplicative factors (multiply base rate by factor)
- Discount tables may use either multiplicative factors or percentage discounts (check structure)
- Vehicle database should be updated annually for new model years
- Territory factors may need periodic review based on claims experience

## Technical Specifications

### File Format
- All files are CSV format with headers
- Encoding: UTF-8
- Delimiter: Comma (,)

### Naming Convention
- Folders: lowercase with underscores
- Files: descriptive names with " - Sheet1.csv" suffix
- Fields: lowercase with underscores where applicable

### Data Types
- Factors: Decimal numbers (typically 0.1 to 5.0 range)
- Codes: Alphanumeric strings
- Limits: Integer or string format (e.g., "30/60", "1000")
- Percentages: Either decimal (0.20) or percentage (20%) format

## Version Control
- Last Updated: 2024
- Data Source: Insurance rating manual and actuarial tables
- Validation: All factors should be positive numbers
- Dependencies: None (self-contained rating structure)

---

For questions about this data structure or specific rating scenarios, consult the actuarial documentation or contact the development team.
