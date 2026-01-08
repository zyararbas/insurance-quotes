import pandas as pd
from typing import Dict, List
from app.utils.data_loader import DataLoader
from app.models.models import Discounts
import logging

logger = logging.getLogger(__name__)

TENURE_FACTOR_LOOKUP = {
    'Less than 3 years': {
        'bipd_factor': 0.00, 'coll_factor': 0.00, 'comp_factor': 0.00, 'mpc_factor': 0.00, 'um_factor': 0.00,
    },
    '3 Years': {
        'bipd_factor': 0.11, 'coll_factor': 0.10, 'comp_factor': 0.08, 'mpc_factor': 0.04, 'um_factor': 0.09,
    },
    '4 Years': {
        'bipd_factor': 0.15, 'coll_factor': 0.12, 'comp_factor': 0.12, 'mpc_factor': 0.12, 'um_factor': 0.12,
    },
    '5 Years': {
        'bipd_factor': 0.17, 'coll_factor': 0.17, 'comp_factor': 0.21, 'mpc_factor': 0.17, 'um_factor': 0.15,
    },
    '6 Years': {
        'bipd_factor': 0.24, 'coll_factor': 0.25, 'comp_factor': 0.29, 'mpc_factor': 0.33, 'um_factor': 0.29,
    },
}


class DiscountService:
    """Service for calculating various discount factors for the pricing calculation."""
    
    def __init__(self, carrier_config: dict):
        self.carrier_config = carrier_config
        self.data_loader = DataLoader()
        self.transportation_network_factors: pd.DataFrame = None
        self.transportation_friends_factors: pd.DataFrame = None
        self.federal_employee_factors: pd.DataFrame = None

    def initialize(self):
        """Loads the discount factor tables into memory."""
        self.transportation_network_factors = self.data_loader.load_transportation_network_factors()
        self.transportation_friends_factors = self.data_loader.load_transportation_friends_factors()
        self.federal_employee_factors = self.data_loader.load_federal_employee_factors()

    def calculate_discount_factors(self, discounts: Discounts, special_factors, coverages: List[str]) -> Dict:
        """
        Calculates all applicable discount factors for each coverage.
        
        Args:
            discounts: Discounts object containing all discount information
            special_factors: SpecialFactors object containing additional factors
            coverages: List of coverage types to calculate factors for
            
        Returns:
            Dictionary with discount factors for each coverage
        """
        if self.transportation_network_factors is None:
            self.initialize()
            
        results = {}
        
        for coverage in coverages:
            coverage_discounts = {}
            
            # 1. Mature Driver Improvement Course Discount
            mature_driver_factor = self._calculate_mature_driver_discount(discounts.mature_driver_course)
            coverage_discounts['mature_driver_course'] = mature_driver_factor # no migration needed
            


            # 2. Loyalty Discount (based on tenure years)
            loyalty_factor = self._calculate_loyalty_discount(discounts.loyalty_years, coverage)
            coverage_discounts['loyalty'] = loyalty_factor
            
            # testing works 

            # 3. Federal Employee Discount
            federal_employee_factor = self._calculate_federal_employee_discount(
                special_factors.federal_employee, coverage
            )
            coverage_discounts['federal_employee'] = federal_employee_factor
            
            # 4. CA Good Driver Discount
            good_driver_factor = self._calculate_good_driver_discount(discounts.good_driver)
            coverage_discounts['good_driver'] = good_driver_factor
            
            # 5. Transportation of Friends Factor
            friends_factor = self._calculate_transportation_friends_factor(
                special_factors.transportation_of_friends, coverage
            )
            coverage_discounts['transportation_friends'] = friends_factor
            
            # 6. Transportation Network Use Factor
            network_factor = self._calculate_transportation_network_factor(
                special_factors.transportation_network_company, coverage
            )
            coverage_discounts['transportation_network'] = network_factor
            
            # 7. Multi-line Discount
            multi_line_factor = self._calculate_multi_line_discount(discounts.multi_line, coverage)
            coverage_discounts['multi_line'] = multi_line_factor
            
            # Calculate combined discount factor
            combined_factor = 1.0
            for factor_name, factor_value in coverage_discounts.items():
                if factor_value != 1.0:  # Only multiply if it's not the default
                    combined_factor *= factor_value
                    
            results[coverage] = {
                "combined_factor": combined_factor,
                "breakdown": coverage_discounts
            }
            
        return results

    def _calculate_mature_driver_discount(self, mature_driver_course: bool) -> float:
        """Calculate mature driver improvement course discount factor."""
        if mature_driver_course:
            return 0.95  # 5% discount
        return 1.0

    def _calculate_loyalty_discount(self, loyalty_years: int, coverage: str) -> float:
        """Calculate loyalty discount based on loyalty years and coverage."""
        try:
            # Load loyalty discount data
            # DEPRECATED loyalty_data = self.data_loader.load_loyalty_discount_factors()
            
            # Map coverage names to CSV column names
            coverage_column_map = {
                'BIPD': 'bipd_factor',
                'COLL': 'coll_factor', 
                'COMP': 'comp_factor',
                'MPC': 'mpc_factor',
                'UM': 'um_factor'
            }
            
            column_name = coverage_column_map.get(coverage, coverage.lower() + '_factor')
            if loyalty_years == 0:
                loyalty_key = "Less than 3 years"

                discount_percentage = (
                    TENURE_FACTOR_LOOKUP
                    .get(loyalty_key, {})
                    .get(column_name)
                )

                if discount_percentage is not None:
                    factor = 1.0 - discount_percentage
                    logger.info(
                        f"Loyalty discount for {coverage} at {loyalty_years} years "
                        f"(Less than 3): {discount_percentage} -> factor {factor}"
                    )
                    return factor

            # ---- 3+ years ----
            elif loyalty_years >= 3:
                loyalty_key = f"{loyalty_years} Years"

                # Try exact match first
                if loyalty_key in TENURE_FACTOR_LOOKUP:
                    discount_percentage = TENURE_FACTOR_LOOKUP[loyalty_key].get(column_name)
                    if discount_percentage is not None:
                        factor = 1.0 - discount_percentage
                        logger.info(
                            f"Loyalty discount for {coverage} at {loyalty_years} years: "
                            f"{discount_percentage} -> factor {factor}"
                        )
                        return factor

                # Fallback to highest available <= loyalty_years
                available_years = sorted(
                    int(k.split()[0])
                    for k in TENURE_FACTOR_LOOKUP
                    if k.endswith("Years")
                    and int(k.split()[0]) <= loyalty_years
                )

                if available_years:
                    max_year = available_years[-1]
                    loyalty_key = f"{max_year} Years"

                    discount_percentage = TENURE_FACTOR_LOOKUP[loyalty_key].get(column_name)
                    if discount_percentage is not None:
                        factor = 1.0 - discount_percentage
                        logger.info(
                            f"Loyalty discount for {coverage} at {loyalty_years} years "
                            f"(using {loyalty_key}): {discount_percentage} -> factor {factor}"
                        )
                        return factor

            # ---- 1–2 years or fallback ----
            logger.info(
                f"Loyalty discount for {coverage} at {loyalty_years} years: No discount (1–2 years)"
            )
            return 1.0
            # DEPRECATED
            # # Handle "Less than 3 years" case (loyalty_years = 0)
            # if loyalty_years == 0:
            #     loyalty_key = "Less than 3 years"
            #     row = loyalty_data[loyalty_data['tenure_years'] == loyalty_key]
                
            #     if not row.empty and column_name in row.columns:
            #         discount_percentage = float(row[column_name].iloc[0])
            #         # Convert discount percentage to factor: 1 - discount_percentage
            #         factor = 1 - discount_percentage
            #         logger.info(f"Loyalty discount for {coverage} at {loyalty_years} years (Less than 3): {discount_percentage} -> factor {factor}")
            #         return factor
            
            # # Handle 3+ years case
            # elif loyalty_years >= 3:
            #     # Try to find exact match first, then fallback to highest available
            #     loyalty_key = f"{loyalty_years} Years"
            #     row = loyalty_data[loyalty_data['tenure_years'] == loyalty_key]
                
            #     # If no exact match, use the highest available tier
            #     if row.empty:
            #         # Get all available years and find the highest one <= loyalty_years
            #         available_years = []
            #         for tenure_str in loyalty_data['tenure_years']:
            #             if 'Years' in tenure_str:
            #                 year_num = int(tenure_str.split()[0])
            #                 if year_num <= loyalty_years:
            #                     available_years.append(year_num)
                    
            #         if available_years:
            #             max_year = max(available_years)
            #             loyalty_key = f"{max_year} Years"
            #             row = loyalty_data[loyalty_data['tenure_years'] == loyalty_key]
                
            #     if not row.empty and column_name in row.columns:
            #         discount_percentage = float(row[column_name].iloc[0])
            #         # Convert discount percentage to factor: 1 - discount_percentage
            #         factor = 1 - discount_percentage
            #         logger.info(f"Loyalty discount for {coverage} at {loyalty_years} years: {discount_percentage} -> factor {factor}")
            #         return factor
            
            # # For any other case (1-2 years), return no discount
            # else:
            #     logger.info(f"Loyalty discount for {coverage} at {loyalty_years} years: No discount (1-2 years)")
            #     return 1.0
                    
        except Exception as e:
            logger.error(f"Error calculating loyalty discount for {coverage}: {e}")
            
        return 1.0

    def _calculate_federal_employee_discount(self, federal_employee: bool, coverage: str) -> float:
        """Calculate federal employee discount factor."""
        if not federal_employee:
            return 1.0
            
        try:
            # Look up federal employee factor - it's the same for all coverages
            # The CSV has 'eligible ' (with trailing space) and 'factor' columns
            eligible_row = self.federal_employee_factors[self.federal_employee_factors['eligible '] == 'Yes']
            if not eligible_row.empty:
                factor = float(eligible_row['factor'].iloc[0])
                logger.info(f"Federal employee discount factor: {factor}")
                return factor
        except (KeyError, IndexError) as e:
            logger.warning(f"Federal employee factor not found: {e}")
            
        return 0.70  # Default 30% discount (to match UI display)

    def _calculate_good_driver_discount(self, good_driver: bool) -> float:
        """Calculate CA good driver discount factor."""
        if good_driver:
            return 0.80  # 20% discount (from CSV: 0.2)
        return 1.0

    def _calculate_transportation_friends_factor(self, transportation_friends: bool, coverage: str) -> float:
        """Calculate transportation of friends factor."""
        if not transportation_friends:
            return 1.0
            
        try:
            # Look up transportation friends factor - it's the same for all coverages
            # The CSV has 'eligbile' (typo) and 'factor' columns
            eligible_row = self.transportation_friends_factors[self.transportation_friends_factors['eligbile'] == 'Yes']
            if not eligible_row.empty:
                factor = float(eligible_row['factor'].iloc[0])
                logger.info(f"Transportation friends factor: {factor}")
                return factor
        except (KeyError, IndexError) as e:
            logger.warning(f"Transportation friends factor not found: {e}")
            
        return 1.2  # Default 20% increase (from CSV)

    def _calculate_transportation_network_factor(self, transportation_network: bool, coverage: str) -> float:
        """Calculate transportation network use factor."""
        if not transportation_network:
            return 1.0
            
        try:
            # Look up transportation network factor for this coverage
            # The CSV has 'Coverage' and 'Factor' columns
            # Map UM to U for CSV compatibility
            coverage_key = 'U' if coverage == 'UM' else coverage
            coverage_row = self.transportation_network_factors[self.transportation_network_factors['Coverage'] == coverage_key]
            if not coverage_row.empty:
                factor = float(coverage_row['Factor'].iloc[0])
                logger.info(f"Transportation network factor for {coverage}: {factor}")
                return factor
        except (KeyError, IndexError) as e:
            logger.warning(f"Transportation network factor not found for coverage {coverage}: {e}")
            
        return 1.15  # Default 15% increase (from CSV)

    def _calculate_multi_line_discount(self, multi_line_type: str, coverage: str) -> float:
        """Calculate multi-line discount factor."""
        if not multi_line_type or multi_line_type == '':
            return 1.0
            
        try:
            # Load multi-line discount data
            multi_line_data = self.data_loader.load_multi_line_discount()
            
            # Look up the discount for the specified multi-line type
            if multi_line_type in multi_line_data.index:
                discount_percentage = float(multi_line_data.loc[multi_line_type, 'discount'])
                # Convert percentage to factor (e.g., 0.04 = 4% discount = 0.96 factor)
                factor = 1.0 - discount_percentage
                logger.info(f"Multi-line discount for {multi_line_type}: {discount_percentage}% -> factor {factor}")
                return factor
            else:
                logger.warning(f"Multi-line discount type '{multi_line_type}' not found in data")
                return 1.0
        except Exception as e:
            logger.error(f"Error calculating multi-line discount: {e}")
            return 1.0
