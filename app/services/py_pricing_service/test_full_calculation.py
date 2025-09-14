#!/usr/bin/env python3
"""
Test script for the full pricing calculation with all factors.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.pricing_orchestrator import PricingOrchestrator
from models.models import (
    RatingInput, Vehicle, Driver, Coverages, Coverage, 
    Discounts, SpecialFactors, Usage
)
from datetime import date

def test_full_calculation():
    """Test the complete pricing calculation with all factors."""
    
    # Create a sample rating input
    rating_input = RatingInput(
        carrier="STATEFARM",
        state="CA",
        zip_code="90210",
        vehicle=Vehicle(
            year=2020,
            make="TOYOTA",
            model="CAMRY",
            series="SE",
            package="",
            style="4D SEDAN",
            engine="2.5L 4CYL",
            msrp=25000.0
        ),
        coverages=Coverages(
            BIPD=Coverage(selected=True, limits="100/300"),
            COLL=Coverage(selected=True, deductible=500),
            COMP=Coverage(selected=True, deductible=500),
            MPC=Coverage(selected=True, limits="5000"),
            UM=Coverage(selected=False)
        ),
        drivers=[
            Driver(
                driver_id="DRIVER001",
                years_licensed=10,
                percentage_use=100.0,
                assigned_driver=True,
                age=35,
                marital_status="M",
                violations=[]
            )
        ],
        discounts=Discounts(
            car_safety_rating="5",
            good_driver=True,
            good_student=False,
            inexperienced_driver_education=False,
            mature_driver_course=True,
            multi_line="Life or Health Insurance",
            student_away_at_school=False,
            loyalty_years=5
        ),
        special_factors=SpecialFactors(
            federal_employee=False,
            transportation_network_company=False,
            transportation_of_friends=False
        ),
        usage=Usage(
            annual_mileage=12000,
            type="Pleasure / Work / School",
            single_automobile=True
        )
    )
    
    # Initialize the pricing orchestrator
    carrier_config = {"carrier": "STATEFARM"}
    orchestrator = PricingOrchestrator(carrier_config)
    
    try:
        # Calculate the premium
        result = orchestrator.calculate_premium(rating_input)
        
        print("✅ Full calculation completed successfully!")
        print(f"Total Premium: ${result['total_premium']:.2f}")
        print("\nCoverage Breakdown:")
        for coverage, premium in result['premiums'].items():
            print(f"  {coverage}: ${premium:.2f}")
        
        print("\nFactor Breakdowns:")
        print("Driver Factors:", result['breakdowns']['driver_factors'])
        print("Model Year Factors:", result['breakdowns']['model_year_factors'])
        print("LRG Factors:", result['breakdowns']['lrg_factors'])
        print("Discount Factors:", result['breakdowns']['discount_factors'])
        
        return result
        
    except Exception as e:
        print(f"❌ Error in calculation: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    test_full_calculation()
