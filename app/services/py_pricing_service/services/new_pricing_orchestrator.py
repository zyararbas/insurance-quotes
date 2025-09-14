import logging
from typing import Dict
from app.services.py_pricing_service.models.models import RatingInput
from app.services.py_pricing_service.services.aggregation_services.coverage_calculation_aggregator import CoverageCalculationAggregator

logger = logging.getLogger(__name__)

class NewPricingOrchestrator:
    """
    New pricing orchestrator that uses the microservices architecture.
    This orchestrator coordinates the individual lookup services and aggregation services
    for better isolation and maintainability.
    """
    
    def __init__(self, carrier_config: dict):
        self.carrier_config = carrier_config
        self.coverage_calculator = CoverageCalculationAggregator(carrier_config)
        
    def initialize(self):
        """Initializes all underlying services."""
        self.coverage_calculator.initialize()
        logger.info("NewPricingOrchestrator initialized")
        
    def calculate_premium(self, rating_input: RatingInput) -> Dict:
        """
        Main method to calculate insurance premium using the microservices architecture.
        
        This orchestrator delegates to the CoverageCalculationAggregator which:
        1. Uses individual lookup services for each type of factor
        2. Aggregates driver factors through DriverAdjustmentAggregator
        3. Calculates final premiums using the complete formula
        
        Returns: Complete calculation results with detailed breakdowns
        """
        if not hasattr(self.coverage_calculator.base_rate_service, 'base_rates'):
            self.initialize()
            
        logger.info("--- Starting Premium Calculation with Microservices Architecture ---")
        
        try:
            # Delegate to the coverage calculation aggregator
            result = self.coverage_calculator.calculate_coverage_premiums(rating_input)
            
            logger.info("--- Premium Calculation Complete ---")
            logger.info(f"Total Premium: {result.get('total_premium', 0)}")
            logger.info(f"Coverages: {list(result.get('premiums', {}).keys())}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error in premium calculation: {e}", exc_info=True)
            raise
            
    def get_driver_adjustment_factors(self, rating_input: RatingInput) -> Dict:
        """
        Gets just the driver adjustment factors for debugging and analysis.
        This isolates the driver calculation logic.
        """
        if not hasattr(self.coverage_calculator.base_rate_service, 'base_rates'):
            self.initialize()
            
        selected_coverages = [cov for cov, details in rating_input.coverages.dict().items() if details]
        
        return self.coverage_calculator.driver_adjustment_aggregator.calculate_driver_adjustment_factors(
            rating_input.drivers,
            rating_input.usage,
            selected_coverages,
            rating_input.discounts
        )
        
    def get_coverage_breakdown(self, coverage: str, rating_input: RatingInput) -> Dict:
        """
        Gets a detailed breakdown for a specific coverage.
        Useful for debugging and step-by-step analysis.
        """
        if not hasattr(self.coverage_calculator.base_rate_service, 'base_rates'):
            self.initialize()
            
        return self.coverage_calculator.get_coverage_breakdown(coverage, rating_input)
        
    def get_individual_factors(self, rating_input: RatingInput) -> Dict:
        """
        Gets all individual factors without aggregation.
        Useful for debugging and understanding the raw lookup values.
        """
        if not hasattr(self.coverage_calculator.base_rate_service, 'base_rates'):
            self.initialize()
            
        selected_coverages = [cov for cov, details in rating_input.coverages.dict().items() if details]
        
        # Get base factors
        base_factors = self.coverage_calculator.base_rate_service.calculate_base_factors(
            rating_input.zip_code, selected_coverages
        )
        
        # Get vehicle factors
        vehicle_factors = self.coverage_calculator.vehicle_factor_service.calculate_vehicle_factors(
            rating_input.vehicle, rating_input.usage, selected_coverages
        )
        
        # Get vehicle rating groups
        vehicle_rating_groups = self.coverage_calculator.vehicle_factor_service.get_vehicle_rating_groups(rating_input.vehicle)
        
        # Get coverage factors
        coverage_factors = self.coverage_calculator.coverage_factor_service.calculate_coverage_factors(
            rating_input.coverages, vehicle_rating_groups
        )
        
        return {
            "base_factors": base_factors,
            "vehicle_factors": vehicle_factors,
            "coverage_factors": coverage_factors,
            "vehicle_rating_groups": vehicle_rating_groups
        }
