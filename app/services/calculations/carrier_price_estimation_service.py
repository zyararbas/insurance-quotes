import logging
from typing import Dict

logger = logging.getLogger(__name__)

class CarrierPriceEstimationService:
    """
    Service for estimating prices for other carriers based on State Farm pricing.
    Uses California (CA) ratio multipliers to estimate competitor prices.
    """
    
    # CA ratio multipliers for estimating other carriers based on State Farm price
    CARRIER_MULTIPLIERS = {
        "GEICO": 0.64,
        "PROGRESSIVE": 0.75,
        "ALLSTATE": 0.92,
        "LIBERTY_MUTUAL": 1.4
    }
    
    def __init__(self, carrier_config: dict):
        self.carrier_config = carrier_config
    
    def initialize(self):
        """Initialization method for consistency with other services."""
        logger.info("CarrierPriceEstimationService initialized")
    
    def estimate_carrier_prices(self, state_farm_total_premium: float) -> Dict[str, float]:
        """
        Estimates prices for other carriers based on State Farm total premium.
        
        Args:
            state_farm_total_premium: The total premium calculated for State Farm
            
        Returns:
            Dictionary with estimated prices for each carrier, keyed by carrier name
        """
        if state_farm_total_premium is None or state_farm_total_premium <= 0:
            logger.warning(f"Invalid State Farm premium: {state_farm_total_premium}")
            return {}
        
        estimates = {}
        
        for carrier_name, multiplier in self.CARRIER_MULTIPLIERS.items():
            estimated_price = state_farm_total_premium * multiplier
            estimates[carrier_name] = round(estimated_price, 2)
            logger.info(f"Estimated {carrier_name} price: ${estimated_price:.2f} (multiplier: {multiplier})")
        
        return estimates
    
    def get_carrier_multipliers(self) -> Dict[str, float]:
        """
        Returns the carrier multipliers dictionary.
        
        Returns:
            Dictionary mapping carrier names to their multipliers
        """
        return self.CARRIER_MULTIPLIERS.copy()

