import logging
from typing import Dict, List
from models.models import RatingInput, Vehicle, Usage, Coverages
from app.services.lookup_services.base_rate_lookup_service import BaseRateLookupService
from app.services.lookup_services.vehicle_factor_lookup_service import VehicleFactorLookupService
from app.services.lookup_services.coverage_factor_lookup_service import CoverageFactorLookupService
from app.services.aggregation_services.driver_adjustment_aggregator import DriverAdjustmentAggregator

logger = logging.getLogger(__name__)

class CoverageCalculationAggregator:
    """
    Service that aggregates all factors to calculate the final coverage premiums.
    This is the core service for Coverage Calculation Step-by-Step.
    """
    
    def __init__(self, carrier_config: dict):
        self.carrier_config = carrier_config
        self.base_rate_service = BaseRateLookupService(carrier_config)
        self.vehicle_factor_service = VehicleFactorLookupService(carrier_config)
        self.coverage_factor_service = CoverageFactorLookupService(carrier_config)
        self.driver_adjustment_aggregator = DriverAdjustmentAggregator(carrier_config)
        
    def initialize(self):
        """Initializes all underlying services."""
        self.base_rate_service.initialize()
        self.vehicle_factor_service.initialize()
        self.coverage_factor_service.initialize()
        self.driver_adjustment_aggregator.initialize()
        logger.info("CoverageCalculationAggregator initialized")
        
    def calculate_coverage_premiums(self, rating_input: RatingInput) -> Dict:
        """
        Calculates the final coverage premiums using the complete formula.
        This method orchestrates all the individual lookups and calculations.
        
        Formula: Driver Adjustment Factor * Base Rate * Model Year Factor * LRG Factor * Coverage Factor * Discount Factors = Premium
        
        Returns: Complete calculation results with breakdowns
        """
        if not hasattr(self.base_rate_service, 'base_rates'):
            self.initialize()
            
        # Get list of selected coverages
        selected_coverages = [cov for cov, details in rating_input.coverages.dict().items() if details]
        
        logger.info(f"Calculating premiums for coverages: {selected_coverages}")
        
        # Step 1: Calculate Base Rates (territory-adjusted)
        logger.info("--- Step 1: Calculating Base Rates ---")
        base_factors = self.base_rate_service.calculate_base_factors(
            rating_input.zip_code, selected_coverages
        )
        logger.info(f"Base factors: {base_factors}")
        
        # Step 2: Calculate Driver Adjustment Factors
        logger.info("--- Step 2: Calculating Driver Adjustment Factors ---")
        driver_adjustment_factors = self.driver_adjustment_aggregator.calculate_driver_adjustment_factors(
            rating_input.drivers,
            rating_input.usage,
            selected_coverages,
            rating_input.discounts
        )
        logger.info(f"Driver adjustment factors: {driver_adjustment_factors}")
        
        # Step 3: Calculate Vehicle Factors
        logger.info("--- Step 3: Calculating Vehicle Factors ---")
        vehicle_factors = self.vehicle_factor_service.calculate_vehicle_factors(
            rating_input.vehicle, rating_input.usage, selected_coverages
        )
        logger.info(f"Vehicle factors: {vehicle_factors}")
        
        # Step 4: Calculate Coverage Factors (deductibles, limits)
        logger.info("--- Step 4: Calculating Coverage Factors ---")
        vehicle_rating_groups = self.vehicle_factor_service.get_vehicle_rating_groups(rating_input.vehicle)
        coverage_factors = self.coverage_factor_service.calculate_coverage_factors(
            rating_input.coverages, vehicle_rating_groups
        )
        logger.info(f"Coverage factors: {coverage_factors}")
        
        # Step 5: Calculate Discount Factors (separate from driver adjustment)
        logger.info("--- Step 5: Calculating Discount Factors ---")
        from services.calculations.discount_service import DiscountService
        discount_service = DiscountService(self.carrier_config)
        discount_service.initialize()
        
        discount_factors = {}
        for coverage in selected_coverages:
            # Calculate discount factors using the DiscountService which handles special factors
            coverage_discounts = discount_service.calculate_discount_factors(
                rating_input.discounts, 
                rating_input.special_factors, 
                [coverage]
            )
            # Extract the breakdown for this coverage
            discount_factors[coverage] = coverage_discounts.get(coverage, {}).get('breakdown', {})
            logger.info(f"Discount factors for {coverage}: {discount_factors[coverage]}")
        
        # Step 6: Assemble Final Premiums using the complete formula
        logger.info("--- Step 6: Assembling Final Premiums ---")
        premiums = {}
        total_premium = 0
        
        for coverage in selected_coverages:
            # Calculate premium using the same step-by-step approach as the frontend
            # Step 1: Base Rate
            base_rate = base_factors.get(coverage, {}).get('base_rate', 0)
            
            # Step 2: Base Rate * Territory Factor
            territory_factor = base_factors.get(coverage, {}).get('territory_factor', 1.0)
            step2_total = base_rate * territory_factor
            
            # Step 3: Step 2 * Coverage Factor
            coverage_factor = coverage_factors.get(coverage, {}).get('factor', 1.0)
            step3_total = step2_total * coverage_factor
            
            # Step 4: Step 3 * Driver Adjustment Factor (without single auto and discounts)
            driver_factor = driver_adjustment_factors.get(coverage, {}).get('drivers', [{}])[0].get('factors', {})
            # Calculate driver factor without single auto (since we apply it separately)
            base_driver_factor = driver_factor.get('base_factor', 1.0)
            years_licensed_factor = driver_factor.get('years_licensed_factor', 1.0)
            percentage_use_factor = driver_factor.get('percentage_use_factor', 1.0)
            safety_record_factor = driver_factor.get('safety_record_factor', 1.0)
            annual_mileage_factor = driver_factor.get('annual_mileage_factor', 1.0)
            usage_type_factor = driver_factor.get('usage_type_factor', 1.0)
            
            driver_combined_factor = (base_driver_factor * years_licensed_factor * percentage_use_factor * 
                                    safety_record_factor * annual_mileage_factor * usage_type_factor)
            step4_total = step3_total * driver_combined_factor
            
            # Step 5: Step 4 * Single Auto Factor
            single_auto_factor = driver_factor.get('single_auto_factor', 1.0)
            step5_total = step4_total * single_auto_factor
            
            # Step 6: Step 5 * Vehicle Factor (Model Year + LRG)
            vehicle_factor = vehicle_factors.get(coverage, {}).get('combined_factor', 1.0)
            step6_total = step5_total * vehicle_factor
            
            # Step 7: Step 6 * LRG Factor (only for BIPD)
            if coverage == 'BIPD':
                lrg_factor = vehicle_factors.get(coverage, {}).get('breakdown', {}).get('lrg_factor', 1.0)
                step7_total = step6_total * lrg_factor
            else:
                step7_total = step6_total
            
            # Steps 8-12: Apply discount factors step by step
            coverage_discounts = discount_factors.get(coverage, {})
            current_total = step7_total
            
            # Step 8: Loyalty Discount
            loyalty_factor = coverage_discounts.get('loyalty', 1.0)
            current_total *= loyalty_factor
            
            # Step 9: Federal Employee Discount
            federal_employee_factor = coverage_discounts.get('federal_employee', 1.0)
            current_total *= federal_employee_factor
            
            # Step 10: Good Driver Discount
            good_driver_factor = coverage_discounts.get('good_driver', 1.0)
            current_total *= good_driver_factor
            
            # Step 11: Transportation Friends Factor
            transportation_friends_factor = coverage_discounts.get('transportation_friends', 1.0)
            current_total *= transportation_friends_factor
            
            # Step 12: Transportation Network Factor
            transportation_network_factor = coverage_discounts.get('transportation_network', 1.0)
            current_total *= transportation_network_factor
            
            # Step 13: Multi-line Discount
            multi_line_factor = coverage_discounts.get('multi_line', 1.0)
            current_total *= multi_line_factor
            
            coverage_premium = current_total
            
            premiums[coverage] = round(coverage_premium, 2)
            total_premium += coverage_premium
            
            logger.info(f"Premium calculation for {coverage}:")
            logger.info(f"  Step 1 - Base Rate: {base_rate}")
            logger.info(f"  Step 2 - Territory Factor: {territory_factor}, Total: {step2_total}")
            logger.info(f"  Step 3 - Coverage Factor: {coverage_factor}, Total: {step3_total}")
            logger.info(f"  Step 4 - Driver Factor: {driver_combined_factor}, Total: {step4_total}")
            logger.info(f"  Step 5 - Single Auto Factor: {single_auto_factor}, Total: {step5_total}")
            logger.info(f"  Step 6 - Vehicle Factor: {vehicle_factor}, Total: {step6_total}")
            logger.info(f"  Step 7 - LRG Factor: {lrg_factor if coverage == 'BIPD' else 'N/A'}, Total: {step7_total}")
            logger.info(f"  Step 8 - Loyalty Discount: {loyalty_factor}")
            logger.info(f"  Step 9 - Federal Employee: {federal_employee_factor}")
            logger.info(f"  Step 10 - Good Driver: {good_driver_factor}")
            logger.info(f"  Step 11 - Transportation Friends: {transportation_friends_factor}")
            logger.info(f"  Step 12 - Transportation Network: {transportation_network_factor}")
            logger.info(f"  Step 13 - Multi-line Discount: {multi_line_factor}")
            logger.info(f"  Final Premium: {coverage_premium}")
        
        # Build the final result
        result = {
            "input": rating_input.dict(),
            "premiums": premiums,
            "total_premium": round(total_premium, 2),
            "breakdowns": {
                "base_factors": base_factors,
                "driver_adjustment_factors": driver_adjustment_factors,
                "vehicle_factors": vehicle_factors,
                "coverage_factors": coverage_factors,
                "vehicle_rating_groups": vehicle_rating_groups,
                "discount_factors": discount_factors
            },
            "calculation_summary": {
                "formula": "Driver Adjustment Factor * Base Rate * Vehicle Factor * Coverage Factor * Discount Factors = Premium",
                "driver_adjustment_factors": {
                    coverage: driver_adjustment_factors[coverage].get('base_combined_factor', 1.0) 
                    for coverage in selected_coverages if coverage in driver_adjustment_factors
                },
                "vehicle_factors": {
                    coverage: vehicle_factors[coverage].get('combined_factor', 1.0) 
                    for coverage in selected_coverages if coverage in vehicle_factors
                },
                "coverage_factors": {
                    coverage: coverage_factors[coverage].get('factor', 1.0) 
                    for coverage in selected_coverages if coverage in coverage_factors
                },
                "discount_factors": discount_factors
            },
            "metadata": {
                "carrier": self.carrier_config.get("carrier"),
                "engine": "python-microservices-v1"
            }
        }
        
        logger.info(f"Final calculation complete. Total premium: {total_premium}")
        return result
        
    def get_coverage_breakdown(self, coverage: str, rating_input: RatingInput) -> Dict:
        """
        Gets a detailed breakdown for a specific coverage.
        Useful for debugging and step-by-step analysis.
        """
        if not hasattr(self.base_rate_service, 'base_rates'):
            self.initialize()
            
        # Get individual factors for this coverage
        base_factors = self.base_rate_service.calculate_base_factors(
            rating_input.zip_code, [coverage]
        )
        
        driver_factors = self.driver_adjustment_aggregator.calculate_driver_adjustment_factors(
            rating_input.drivers,
            rating_input.usage,
            [coverage],
            rating_input.discounts
        )
        
        vehicle_factors = self.vehicle_factor_service.calculate_vehicle_factors(
            rating_input.vehicle, rating_input.usage, [coverage]
        )
        
        vehicle_rating_groups = self.vehicle_factor_service.get_vehicle_rating_groups(rating_input.vehicle)
        coverage_factors = self.coverage_factor_service.calculate_coverage_factors(
            rating_input.coverages, vehicle_rating_groups
        )
        
        return {
            "coverage": coverage,
            "base_factors": base_factors.get(coverage, {}),
            "driver_factors": driver_factors.get(coverage, {}),
            "vehicle_factors": vehicle_factors.get(coverage, {}),
            "coverage_factors": coverage_factors.get(coverage, {}),
            "vehicle_rating_groups": vehicle_rating_groups
        }
