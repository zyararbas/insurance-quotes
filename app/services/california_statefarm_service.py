"""
California State Farm Pricing Service

This service handles pricing calculations specifically for California State Farm insurance policies.
It processes policy information and returns pricing quotes based on California regulations
and State Farm's pricing algorithms.
"""

from typing import Dict, Any, Optional
import json
import os
from datetime import datetime


class CaliforniaStateFarmPricingService:
    """
    Service for calculating California State Farm insurance quotes.
    
    This service handles:
    - California-specific regulations and requirements
    - State Farm pricing algorithms
    - Risk assessment for California drivers
    - Coverage calculations
    """
    
    def __init__(self):
        """Initialize the pricing service with California State Farm data."""
        self.data_path = os.path.join(os.path.dirname(__file__), '..', 'data')
        self.load_pricing_data()
    
    def load_pricing_data(self):
        """Load California State Farm pricing data and lookup tables."""
        # This will be populated when you add your data files
        self.pricing_tables = {}
        self.rate_factors = {}
        self.coverage_options = {}
        
        # Placeholder for data loading
        print("Loading California State Farm pricing data...")
    
    def calculate_quote(self, policy_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate insurance quote for California State Farm policy.
        
        Args:
            policy_data: Dictionary containing policy information including:
                - driver_info: Driver details (age, location, driving history)
                - vehicle_info: Vehicle details (year, make, model, VIN)
                - coverage_options: Desired coverage levels
                - discounts: Applicable discounts
        
        Returns:
            Dictionary containing quote details including:
                - premium_amount: Total premium cost
                - coverage_breakdown: Cost breakdown by coverage type
                - discounts_applied: List of applied discounts
                - effective_date: When coverage begins
                - quote_id: Unique quote identifier
        """
        try:
            # Validate input data
            self._validate_policy_data(policy_data)
            
            # Calculate base premium
            base_premium = self._calculate_base_premium(policy_data)
            
            # Apply California-specific factors
            ca_factors = self._apply_california_factors(policy_data, base_premium)
            
            # Apply State Farm specific adjustments
            statefarm_adjustments = self._apply_statefarm_adjustments(policy_data, ca_factors)
            
            # Apply discounts
            final_premium = self._apply_discounts(policy_data, statefarm_adjustments)
            
            # Generate quote response
            quote = self._generate_quote_response(policy_data, final_premium)
            
            return quote
            
        except Exception as e:
            return {
                "error": f"Failed to calculate quote: {str(e)}",
                "quote_id": None,
                "premium_amount": 0
            }
    
    def _validate_policy_data(self, policy_data: Dict[str, Any]) -> None:
        """Validate that required policy data is present."""
        required_fields = ['driver_info', 'vehicle_info', 'coverage_options']
        
        for field in required_fields:
            if field not in policy_data:
                raise ValueError(f"Missing required field: {field}")
    
    def _calculate_base_premium(self, policy_data: Dict[str, Any]) -> float:
        """Calculate base premium before adjustments."""
        # Placeholder calculation - replace with actual State Farm algorithms
        base_rate = 500.0  # Base rate for California
        
        # Adjust for vehicle value, driver age, etc.
        vehicle_value = policy_data.get('vehicle_info', {}).get('value', 20000)
        driver_age = policy_data.get('driver_info', {}).get('age', 30)
        
        # Simple calculation (replace with actual State Farm logic)
        base_premium = base_rate + (vehicle_value * 0.02) + (max(0, 25 - driver_age) * 50)
        
        return base_premium
    
    def _apply_california_factors(self, policy_data: Dict[str, Any], base_premium: float) -> float:
        """Apply California-specific regulatory factors."""
        # California-specific adjustments
        ca_multiplier = 1.15  # California tends to have higher rates
        
        # Location-based adjustments within California
        location = policy_data.get('driver_info', {}).get('location', '')
        if 'los angeles' in location.lower() or 'san francisco' in location.lower():
            ca_multiplier *= 1.2  # Higher rates in major cities
        
        return base_premium * ca_multiplier
    
    def _apply_statefarm_adjustments(self, policy_data: Dict[str, Any], premium: float) -> float:
        """Apply State Farm specific pricing adjustments."""
        # State Farm specific factors
        statefarm_multiplier = 1.0
        
        # Multi-policy discounts
        if policy_data.get('multi_policy', False):
            statefarm_multiplier *= 0.9
        
        # Good driver discounts
        driving_record = policy_data.get('driver_info', {}).get('driving_record', 'good')
        if driving_record == 'excellent':
            statefarm_multiplier *= 0.85
        
        return premium * statefarm_multiplier
    
    def _apply_discounts(self, policy_data: Dict[str, Any], premium: float) -> Dict[str, Any]:
        """Apply applicable discounts and return final pricing."""
        discounts = []
        final_premium = premium
        
        # Safe driver discount
        if policy_data.get('driver_info', {}).get('safe_driver', False):
            discount_amount = premium * 0.1
            final_premium -= discount_amount
            discounts.append({
                "name": "Safe Driver Discount",
                "amount": discount_amount
            })
        
        # Multi-car discount
        if policy_data.get('multi_car', False):
            discount_amount = premium * 0.05
            final_premium -= discount_amount
            discounts.append({
                "name": "Multi-Car Discount", 
                "amount": discount_amount
            })
        
        return {
            "premium_amount": round(final_premium, 2),
            "discounts_applied": discounts,
            "total_discounts": sum(d['amount'] for d in discounts)
        }
    
    def _generate_quote_response(self, policy_data: Dict[str, Any], pricing_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate the final quote response."""
        quote_id = f"CA-SF-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        return {
            "quote_id": quote_id,
            "carrier": "State Farm",
            "state": "California",
            "premium_amount": pricing_data["premium_amount"],
            "coverage_breakdown": {
                "liability": round(pricing_data["premium_amount"] * 0.4, 2),
                "collision": round(pricing_data["premium_amount"] * 0.35, 2),
                "comprehensive": round(pricing_data["premium_amount"] * 0.25, 2)
            },
            "discounts_applied": pricing_data["discounts_applied"],
            "total_discounts": pricing_data["total_discounts"],
            "effective_date": datetime.now().strftime('%Y-%m-%d'),
            "quote_expires": (datetime.now().replace(day=datetime.now().day + 30)).strftime('%Y-%m-%d'),
            "policy_data": {
                "driver_info": policy_data.get('driver_info', {}),
                "vehicle_info": policy_data.get('vehicle_info', {}),
                "coverage_options": policy_data.get('coverage_options', {})
            }
        }
