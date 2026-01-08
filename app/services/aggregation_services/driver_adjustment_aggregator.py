import logging
from typing import Dict, List
from app.models.models import Driver, Usage, Discounts
from app.services.lookup_services.driver_factor_lookup_service import DriverFactorLookupService
from app.services.calculations.safety_record_service import SafetyRecordService

logger = logging.getLogger(__name__)

class DriverAdjustmentAggregator:
    """
    Service that aggregates all driver-related factors to calculate the Driver Adjustment Factor.
    This is the core service for Step-by-Step Driver Adjustment calculations.
    """
    
    def __init__(self, carrier_config: dict):
        self.carrier_config = carrier_config
        self.driver_lookup_service = DriverFactorLookupService(carrier_config)
        self.safety_record_service = SafetyRecordService(carrier_config)
        
    def initialize(self):
        """Initializes all underlying services."""
        self.driver_lookup_service.initialize()
        self.safety_record_service.initialize()
        logger.info("DriverAdjustmentAggregator initialized")
        
    def calculate_driver_adjustment_factors(self, drivers: List[Driver], usage: Usage, 
                                         coverages: List[str], discounts: Discounts) -> Dict:
        """
        Calculates comprehensive driver adjustment factors for all drivers and coverages.
        This is the main method that aggregates all driver-related factors.
        
        Returns: {coverage: {'driver_adjustment_factor': float, 'breakdown': dict, 'drivers': list}}
        """
        if not hasattr(self.driver_lookup_service, 'base_driver_factors'):
            self.initialize()
            
        results = {}
        
        for coverage in coverages:
            # Calculate factors for each driver
            driver_results = []
            total_combined_factor = 1.0
            
            for driver in drivers:
                driver_factor_breakdown = {}
                
                # 1. Base driver factor 
                base_factor = self.driver_lookup_service.get_base_driver_factor(coverage, driver) # Updated to query mongo instea of CSV
                driver_factor_breakdown['base_factor'] = base_factor
                
                
                
                # 2. Years licensed adjustment factor
                years_licensed_factor = self.driver_lookup_service.get_years_licensed_factor(coverage, driver) 
                driver_factor_breakdown['years_licensed_factor'] = years_licensed_factor

               
                
                # 3. Percentage use by driver factor
                percentage_use_factor = self.driver_lookup_service.get_percentage_use_factor(coverage, driver)
                driver_factor_breakdown['percentage_use_factor'] = percentage_use_factor
                
               

                # 4. Driving safety record factor (always calculate from violations)
                # Safety record level is always calculated from violations, never from provided value
                calculated_safety_level = self.safety_record_service.calculate_safety_record_level(driver)
                driver_factor_breakdown['calculated_safety_level'] = calculated_safety_level
                safety_record_factor = self.driver_lookup_service.get_safety_record_factor(coverage, calculated_safety_level)
                
                driver_factor_breakdown['safety_record_factor'] = safety_record_factor
                

                

                # 5. Single Automobile Factor
                single_auto_factor = self.driver_lookup_service.get_single_automobile_factor(coverage, usage)
                driver_factor_breakdown['single_auto_factor'] = single_auto_factor

                


                # 6. Annual Mileage Factor (part of driver adjustment factor)
                annual_mileage_factor = self.driver_lookup_service.get_annual_mileage_factor(coverage, usage)
                driver_factor_breakdown['annual_mileage_factor'] = annual_mileage_factor

                

                # 7. Usage Type Factor (part of driver adjustment factor)
                usage_type_factor = self.driver_lookup_service.get_usage_type_factor(coverage, usage)
                driver_factor_breakdown['usage_type_factor'] = usage_type_factor

                # Calculate combined driver factor (including mileage and usage type)
                driver_combined = (
                    base_factor * 
                    years_licensed_factor * 
                    percentage_use_factor * 
                    safety_record_factor *
                    single_auto_factor *
                    annual_mileage_factor *
                    usage_type_factor
                )
                
                # Store the exact value without rounding for calculations
                driver_factor_breakdown['driver_combined_factor'] = driver_combined
                
                driver_results.append({
                    'driver_id': driver.id,
                    'factors': driver_factor_breakdown
                })
                
                # Add to total combined factor (multiplicative for multiple drivers)
                total_combined_factor *= driver_combined
            
            # 8. Calculate discount factors (but don't apply them here - they'll be applied separately)
            discount_factors = self.driver_lookup_service.calculate_discount_factors(coverage, discounts)
            
            # Use the base combined factor without discounts for the driver adjustment factor
            final_factor = total_combined_factor
            
            # Extract safety record information for display
            safety_info = {}
            for driver_result in driver_results:
                driver_factors = driver_result['factors']
                # Safety record level is always calculated from violations
                if 'calculated_safety_level' in driver_factors:
                    safety_info[driver_result['driver_id']] = {
                        'safety_record_level': driver_factors['calculated_safety_level'],
                        'calculated': True
                    }
            
            results[coverage] = {
                "driver_adjustment_factor": final_factor,  # This is the key field for the final calculation
                "base_combined_factor": total_combined_factor,  # Driver factors without discounts
                "discount_factors": discount_factors,  # Discount factors (calculated but not applied)
                "drivers": driver_results,
                "safety_record_info": safety_info,
                "breakdown": {
                    "method": "state_farm_comprehensive_logic",
                    "driver_count": len(drivers),
                    "factors_applied": ["base_factor", "years_licensed", "percentage_use", "safety_record", "single_auto", "annual_mileage", "usage_type"]
                }
            }
            
            logger.info(f"Driver adjustment factor for {coverage}: {final_factor}")
            
        return results
        
    def get_driver_adjustment_factor_only(self, coverage: str, drivers: List[Driver], 
                                        usage: Usage, discounts: Discounts) -> float:
        """
        Gets just the driver adjustment factor for a specific coverage.
        This is a convenience method for when you only need the final factor.
        """
        coverage_results = self.calculate_driver_adjustment_factors([coverage], drivers, usage, discounts)
        return coverage_results.get(coverage, {}).get('driver_adjustment_factor', 1.0)
