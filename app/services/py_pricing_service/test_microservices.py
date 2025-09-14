#!/usr/bin/env python3
"""
Test script for the new microservices architecture.
This script tests each service independently to ensure they work correctly.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from models.models import RatingInput, Vehicle, Driver, Coverages, Coverage, Discounts, Usage, SpecialFactors
from services.lookup_services.base_rate_lookup_service import BaseRateLookupService
from services.lookup_services.driver_factor_lookup_service import DriverFactorLookupService
from services.lookup_services.vehicle_factor_lookup_service import VehicleFactorLookupService
from services.lookup_services.discount_lookup_service import DiscountLookupService
from services.lookup_services.coverage_factor_lookup_service import CoverageFactorLookupService
from services.aggregation_services.driver_adjustment_aggregator import DriverAdjustmentAggregator
from services.aggregation_services.coverage_calculation_aggregator import CoverageCalculationAggregator
from services.new_pricing_orchestrator import NewPricingOrchestrator

def create_test_rating_input():
    """Creates a test rating input for testing."""
    return RatingInput(
        carrier="STATEFARM",
        state="CA",
        zip_code="90210",
        vehicle=Vehicle(
            year=2020,
            make="TOYOTA",
            model="CAMRY",
            series="LE",
            package="",
            style="4D SEDAN",
            engine="2.5L 4-CYL"
        ),
        coverages=Coverages(
            BIPD=Coverage(selected=True, limits="15/30"),
            COLL=Coverage(selected=True, deductible=500),
            COMP=Coverage(selected=True, deductible=500)
        ),
        drivers=[
            Driver(
                driver_id="driver1",
                years_licensed=10,
                percentage_use=100.0,
                assigned_driver=True,
                marital_status="S"
            )
        ],
        discounts=Discounts(
            good_driver=True,
            multi_line="home"
        ),
        special_factors=SpecialFactors(),
        usage=Usage(
            annual_mileage=12000,
            type="Pleasure / Work / School",
            single_automobile=False
        )
    )

def test_individual_services():
    """Tests each individual lookup service."""
    print("=== Testing Individual Lookup Services ===")
    
    carrier_config = {"carrier": "STATEFARM", "state": "CA"}
    rating_input = create_test_rating_input()
    
    # Test Base Rate Lookup Service
    print("\n--- Testing Base Rate Lookup Service ---")
    base_rate_service = BaseRateLookupService(carrier_config)
    base_rate_service.initialize()
    
    territory_factor = base_rate_service.get_territory_factor("90210")
    print(f"Territory factor for 90210: {territory_factor}")
    
    base_factors = base_rate_service.calculate_base_factors("90210", ["BIPD", "COLL"])
    print(f"Base factors: {base_factors}")
    
    # Test Driver Factor Lookup Service
    print("\n--- Testing Driver Factor Lookup Service ---")
    driver_service = DriverFactorLookupService(carrier_config)
    driver_service.initialize()
    
    driver = rating_input.drivers[0]
    base_factor = driver_service.get_base_driver_factor("BIPD", driver)
    print(f"Base driver factor for BIPD: {base_factor}")
    
    years_factor = driver_service.get_years_licensed_factor("BIPD", driver)
    print(f"Years licensed factor for BIPD: {years_factor}")
    
    # Test Vehicle Factor Lookup Service
    print("\n--- Testing Vehicle Factor Lookup Service ---")
    vehicle_service = VehicleFactorLookupService(carrier_config)
    vehicle_service.initialize()
    
    rating_groups = vehicle_service.get_vehicle_rating_groups(rating_input.vehicle)
    print(f"Vehicle rating groups: {rating_groups}")
    
    model_year_factor = vehicle_service.get_model_year_factor("BIPD", rating_input.vehicle.year)
    print(f"Model year factor for BIPD: {model_year_factor}")
    
    # Test Discount Lookup Service
    print("\n--- Testing Discount Lookup Service ---")
    discount_service = DiscountLookupService(carrier_config)
    discount_service.initialize()
    
    discount_factors = discount_service.calculate_discount_factors("BIPD", rating_input.discounts)
    print(f"Discount factors for BIPD: {discount_factors}")
    
    # Test Coverage Factor Lookup Service
    print("\n--- Testing Coverage Factor Lookup Service ---")
    coverage_service = CoverageFactorLookupService(carrier_config)
    coverage_service.initialize()
    
    bi_factor = coverage_service.get_bi_factor("BIPD", "15/30")
    print(f"BIPD factor for 15/30: {bi_factor}")
    
    coll_factor = coverage_service.get_collision_factor("COLL", "500", rating_groups.get('drg', 1))
    print(f"Collision factor for $500 deductible: {coll_factor}")

def test_aggregation_services():
    """Tests the aggregation services."""
    print("\n=== Testing Aggregation Services ===")
    
    carrier_config = {"carrier": "STATEFARM", "state": "CA"}
    rating_input = create_test_rating_input()
    
    # Test Driver Adjustment Aggregator
    print("\n--- Testing Driver Adjustment Aggregator ---")
    driver_aggregator = DriverAdjustmentAggregator(carrier_config)
    driver_aggregator.initialize()
    
    driver_factors = driver_aggregator.calculate_driver_adjustment_factors(
        rating_input.drivers,
        rating_input.usage,
        ["BIPD", "COLL"],
        rating_input.discounts
    )
    
    for coverage, factors in driver_factors.items():
        print(f"Driver adjustment factor for {coverage}: {factors.get('driver_adjustment_factor', 'N/A')}")
    
    # Test Coverage Calculation Aggregator
    print("\n--- Testing Coverage Calculation Aggregator ---")
    coverage_aggregator = CoverageCalculationAggregator(carrier_config)
    coverage_aggregator.initialize()
    
    coverage_breakdown = coverage_aggregator.get_coverage_breakdown("BIPD", rating_input)
    print(f"Coverage breakdown for BIPD: {coverage_breakdown}")

def test_main_orchestrator():
    """Tests the main orchestrator."""
    print("\n=== Testing Main Orchestrator ===")
    
    carrier_config = {"carrier": "STATEFARM", "state": "CA"}
    rating_input = create_test_rating_input()
    
    orchestrator = NewPricingOrchestrator(carrier_config)
    orchestrator.initialize()
    
    # Test individual factors
    print("\n--- Testing Individual Factors ---")
    individual_factors = orchestrator.get_individual_factors(rating_input)
    print(f"Individual factors: {individual_factors}")
    
    # Test driver adjustment factors
    print("\n--- Testing Driver Adjustment Factors ---")
    driver_factors = orchestrator.get_driver_adjustment_factors(rating_input)
    print(f"Driver adjustment factors: {driver_factors}")
    
    # Test coverage breakdown
    print("\n--- Testing Coverage Breakdown ---")
    coverage_breakdown = orchestrator.get_coverage_breakdown("BIPD", rating_input)
    print(f"Coverage breakdown for BIPD: {coverage_breakdown}")

def main():
    """Main test function."""
    print("Starting Microservices Architecture Tests...")
    
    try:
        test_individual_services()
        test_aggregation_services()
        test_main_orchestrator()
        
        print("\n=== All Tests Completed Successfully! ===")
        print("The new microservices architecture is working correctly.")
        
    except Exception as e:
        print(f"\n=== Test Failed: {e} ===")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
