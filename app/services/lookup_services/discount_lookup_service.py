import pandas as pd
import logging
from typing import Dict, List
from app.utils.data_loader import DataLoader
from app.models.models import Discounts

logger = logging.getLogger(__name__)

class DiscountLookupService:
    """
    Microservice for discount factor lookups.
    Handles all discount-related factor calculations.
    """
    
    def __init__(self, carrier_config: dict):
        self.carrier_config = carrier_config
        self.data_loader = DataLoader()
        
        # Discount tables
        self.good_driver_discount: pd.DataFrame = None
        self.good_student_discount: pd.DataFrame = None
        self.inexperienced_driver_discount: pd.DataFrame = None
        self.mature_driver_discount: pd.DataFrame = None
        self.multi_line_discount: pd.DataFrame = None
        self.student_away_discount: pd.DataFrame = None
        self.loyalty_discount: pd.DataFrame = None
        self.car_safety_discount: pd.DataFrame = None
        
    def initialize(self):
        """Loads all discount data tables."""
        self.good_driver_discount = self.data_loader.load_good_driver_discount()
        self.good_student_discount = self.data_loader.load_good_student_discount()
        self.inexperienced_driver_discount = self.data_loader.load_inexperienced_driver_education_discount()
        self.mature_driver_discount = self.data_loader.load_mature_driver_course_discount()
        self.multi_line_discount = self.data_loader.load_multi_line_discount()
        self.student_away_discount = self.data_loader.load_student_away_discount()
        self.loyalty_discount = self.data_loader.load_loyalty_discount()
        self.car_safety_discount = self.data_loader.load_car_safety_rating_discount()
        
        logger.info("DiscountLookupService initialized")
        
    def get_good_driver_discount(self, coverage: str) -> float:
        """Gets the good driver discount factor."""
        if self.good_driver_discount is None:
            self.initialize()
            
        try:
            factor = self.good_driver_discount.loc['yes', 'factor']
            discount_factor = 1 - float(factor)  # Convert discount to factor
            logger.info(f"Good driver discount factor for {coverage}: {discount_factor}")
            return discount_factor
        except (KeyError, IndexError):
            logger.warning(f"No good driver discount found for {coverage}")
            return 1.0
            
    def get_good_student_discount(self, coverage: str) -> float:
        """Gets the good student discount factor."""
        if self.good_student_discount is None:
            self.initialize()
            
        try:
            discount_pct = self.good_student_discount.loc['yes', 'discount']
            if isinstance(discount_pct, str) and '%' in discount_pct:
                discount_value = float(discount_pct.rstrip('%')) / 100
                discount_factor = 1 - discount_value
                logger.info(f"Good student discount factor for {coverage}: {discount_factor}")
                return discount_factor
        except (KeyError, IndexError):
            logger.warning(f"No good student discount found for {coverage}")
            return 1.0
            
        return 1.0
        
    def get_inexperienced_driver_discount(self, coverage: str) -> float:
        """Gets the inexperienced driver safety education discount factor."""
        if self.inexperienced_driver_discount is None:
            self.initialize()
            
        try:
            factor = self.inexperienced_driver_discount.loc[coverage, 'discount_factor']
            logger.info(f"Inexperienced driver discount factor for {coverage}: {factor}")
            return float(factor)
        except (KeyError, IndexError):
            logger.warning(f"No inexperienced driver discount found for {coverage}")
            return 1.0
            
    def get_mature_driver_discount(self, coverage: str) -> float:
        """Gets the mature driver improvement course discount factor."""
        if self.mature_driver_discount is None:
            self.initialize()
            
        try:
            factor = self.mature_driver_discount.loc['yes', 'factor']
            discount_factor = 1 - float(factor)
            logger.info(f"Mature driver discount factor for {coverage}: {discount_factor}")
            return discount_factor
        except (KeyError, IndexError):
            logger.warning(f"No mature driver discount found for {coverage}")
            return 1.0
            
    def get_student_away_discount(self, coverage: str) -> float:
        """Gets the student away at school discount factor."""
        if self.student_away_discount is None:
            self.initialize()
            
        try:
            factor = self.student_away_discount.loc[coverage, 'discount_factor']
            logger.info(f"Student away discount factor for {coverage}: {factor}")
            return float(factor)
        except (KeyError, IndexError):
            logger.warning(f"No student away discount found for {coverage}")
            return 1.0
            
    def get_multi_line_discount(self, coverage: str, multi_line_type: str) -> float:
        """Gets the multi-line discount factor."""
        if self.multi_line_discount is None:
            self.initialize()
            
        try:
            discount_value = self.multi_line_discount.loc[multi_line_type, 'discount']
            discount_factor = 1 - float(discount_value)
            logger.info(f"Multi-line discount factor for {coverage}: {discount_factor}")
            return discount_factor
        except (KeyError, IndexError):
            logger.warning(f"No multi-line discount found for {coverage}")
            return 1.0
            
    def get_loyalty_discount(self, coverage: str, loyalty_years: int) -> float:
        """Gets the loyalty discount factor based on years of coverage."""
        if self.loyalty_discount is None:
            self.initialize()
            
        if loyalty_years < 3:
            return 1.0
            
        try:
            # Find the appropriate loyalty tier
            loyalty_key = f"{loyalty_years} Years"
            coverage_col = f"{coverage.lower()}_factor"
            factor = self.loyalty_discount.loc[loyalty_key, coverage_col]
            discount_factor = 1 - float(factor)
            logger.info(f"Loyalty discount factor for {coverage} at {loyalty_years} years: {discount_factor}")
            return discount_factor
        except (KeyError, IndexError):
            # Try fallback to highest available tier
            try:
                max_years = max([int(idx.split()[0]) for idx in self.loyalty_discount.index if 'Years' in idx])
                if loyalty_years >= max_years:
                    fallback_key = f"{max_years} Years"
                    coverage_col = f"{coverage.lower()}_factor"
                    factor = self.loyalty_discount.loc[fallback_key, coverage_col]
                    discount_factor = 1 - float(factor)
                    logger.info(f"Loyalty discount fallback factor for {coverage}: {discount_factor}")
                    return discount_factor
            except (KeyError, IndexError, ValueError):
                pass
                
        logger.warning(f"No loyalty discount found for {coverage}")
        return 1.0
        
    def get_car_safety_discount(self, coverage: str, safety_rating: str) -> float:
        """Gets the car safety rating discount factor."""
        if self.car_safety_discount is None:
            self.initialize()
            
        try:
            discount_value = self.car_safety_discount.loc[safety_rating, 'discount']
            discount_factor = 1 - float(discount_value)
            logger.info(f"Car safety discount factor for {coverage}: {discount_factor}")
            return discount_factor
        except (KeyError, IndexError):
            logger.warning(f"No car safety discount found for {coverage}")
            return 1.0
            
    def calculate_discount_factors(self, coverage: str, discounts: Discounts) -> Dict[str, float]:
        """
        Calculates all applicable discount factors for a given coverage.
        Returns: {discount_name: discount_factor}
        """
        if self.good_driver_discount is None:
            self.initialize()
            
        discount_factors = {}
        
        # Good driver discount
        if discounts.good_driver:
            factor = self.get_good_driver_discount(coverage)
            if factor != 1.0:
                discount_factors['good_driver'] = factor
        
        # Good student discount
        if discounts.good_student:
            factor = self.get_good_student_discount(coverage)
            if factor != 1.0:
                discount_factors['good_student'] = factor
        
        # Inexperienced driver safety education discount
        if discounts.inexperienced_driver_education:
            factor = self.get_inexperienced_driver_discount(coverage)
            if factor != 1.0:
                discount_factors['inexperienced_driver_education'] = factor
        
        # Mature driver improvement course discount
        if discounts.mature_driver_course:
            factor = self.get_mature_driver_discount(coverage)
            if factor != 1.0:
                discount_factors['mature_driver_course'] = factor
        
        # Student away at school discount
        if discounts.student_away_at_school:
            factor = self.get_student_away_discount(coverage)
            if factor != 1.0:
                discount_factors['student_away_at_school'] = factor
        
        # Multi-line discount
        if discounts.multi_line:
            factor = self.get_multi_line_discount(coverage, discounts.multi_line)
            if factor != 1.0:
                discount_factors['multi_line'] = factor
        
        # Loyalty discount by years of coverage
        if discounts.loyalty_years >= 3:
            factor = self.get_loyalty_discount(coverage, discounts.loyalty_years)
            if factor != 1.0:
                discount_factors['loyalty'] = factor
        
        # Car safety rating discount
        if discounts.car_safety_rating:
            factor = self.get_car_safety_discount(coverage, discounts.car_safety_rating)
            if factor != 1.0:
                discount_factors['car_safety_rating'] = factor
        
        logger.info(f"Discount factors for {coverage}: {discount_factors}")
        return discount_factors
        
    def calculate_combined_discount_factor(self, coverage: str, discounts: Discounts) -> float:
        """
        Calculates the combined discount factor for a coverage.
        Returns the product of all applicable discounts.
        """
        discount_factors = self.calculate_discount_factors(coverage, discounts)
        
        if not discount_factors:
            return 1.0
            
        combined_factor = 1.0
        for discount_name, factor in discount_factors.items():
            combined_factor *= factor
            
        logger.info(f"Combined discount factor for {coverage}: {combined_factor}")
        return combined_factor

    def get_all_discount_factors(self, discounts: Discounts) -> dict:
        """
        Gets discount factors for all coverages.
        Returns: {coverage: {discount_name: factor}}
        """
        if self.good_driver_discount is None:
            self.initialize()
            
        try:
            coverages = ['BIPD', 'COLL', 'COMP', 'MPC', 'UM']
            all_factors = {}
            
            for coverage in coverages:
                discount_factors = self.calculate_discount_factors(coverage, discounts)
                # Map UM to U for frontend compatibility
                key = 'U' if coverage == 'UM' else coverage
                all_factors[key] = discount_factors
            
            result = {
                'discounts': discounts.dict(),
                'factors': all_factors,
                'source_table': 'various_discount_tables'
            }
            
            logger.info(f"All discount factors: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error getting all discount factors: {e}")
            return {
                'discounts': discounts.dict(),
                'factors': {'BIPD': {}, 'COLL': {}, 'COMP': {}, 'MPC': {}, 'U': {}},
                'source_table': 'various_discount_tables'
            }