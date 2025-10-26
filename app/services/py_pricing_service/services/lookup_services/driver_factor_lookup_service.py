import pandas as pd
import logging
from typing import Dict, List, Optional
from utils.data_loader import DataLoader
from models.models import Driver, Usage, Discounts
from services.calculations.discount_service import DiscountService

logger = logging.getLogger(__name__)

class DriverFactorLookupService:
    """
    Microservice for driver factor lookups.
    Handles all driver-related factor calculations including safety records.
    """
    
    def __init__(self, carrier_config: dict):
        self.carrier_config = carrier_config
        self.data_loader = DataLoader()
        self.discount_service = DiscountService(carrier_config)
        
        # Driver factor tables
        self.base_driver_factors: pd.DataFrame = None
        self.years_licensed_factors: pd.DataFrame = None
        self.percentage_use_factors: pd.DataFrame = None
        self.driving_safety_record_factors: pd.DataFrame = None
        self.single_auto_factors: pd.DataFrame = None
        
        # Usage-related tables
        self.annual_mileage_factors: pd.DataFrame = None
        self.usage_type_factors: pd.DataFrame = None
        
    def initialize(self):
        """Loads all driver factor data tables."""
        self.base_driver_factors = self.data_loader.load_base_driver_factors()
        self.years_licensed_factors = self.data_loader.load_years_licensed_key()
        self.percentage_use_factors = self.data_loader.load_percentage_use_by_driver()
        self.driving_safety_record_factors = self.data_loader.load_driving_safety_record_rating_plan()
        self.single_auto_factors = self.data_loader.load_single_auto_factors()
        self.annual_mileage_factors = self.data_loader.load_annual_mileage_factors()
        self.usage_type_factors = self.data_loader.load_usage_type_factors()
        
        logger.info("DriverFactorLookupService initialized")
        
    def get_base_driver_factor(self, coverage: str, driver: Driver) -> float:
        """Gets the base driver factor from the rating tables."""
        if self.base_driver_factors is None:
            self.initialize()
            
        assigned_driver_str = 'Yes' if driver.assigned_driver else 'No'
        
        # Use the same logic as the working old service
        if driver.assigned_driver:
            if driver.years_licensed <= 13:
                lookup_marital_status = driver.marital_status if driver.marital_status in ['S', 'M'] else 'S'
                lookup_years_driving = str(driver.years_licensed)
            else: # 14+ years
                lookup_marital_status = 'All Not\n Specifically\n Listed'  # Note the \n characters
                lookup_years_driving = str(driver.years_licensed)
        else: # Not assigned
            lookup_marital_status = 'All Not\n Specifically\n Listed'  # Note the \n characters
            lookup_years_driving = 'All Not\n Specifically\n Listed'  # Note the \n characters
        
        # Handle coverage name mapping (UM -> U in base driver factors table)
        coverage_key = coverage
        if coverage == 'UM':
            coverage_key = 'U'
        
        # Find matching row
        match = self.base_driver_factors[
            (self.base_driver_factors['Coverage'] == coverage_key) &
            (self.base_driver_factors['Assigned Driver'] == assigned_driver_str) &
            (self.base_driver_factors['Marital Status'] == lookup_marital_status) &
            (self.base_driver_factors['Years Driving'] == lookup_years_driving)
        ]

        if not match.empty:
            factor = float(match['Factor'].iloc[0])
            logger.info(f"Base driver factor for {coverage}: {factor}")
            return factor
        else:
            # Fallback logic - use the same format as the working old service
            fallback = self.base_driver_factors[
                (self.base_driver_factors['Coverage'] == coverage_key) &
                (self.base_driver_factors['Assigned Driver'] == assigned_driver_str) &
                (self.base_driver_factors['Marital Status'] == 'All Not\n Specifically\n Listed') &
                (self.base_driver_factors['Years Driving'] == 'All Not\n Specifically\n Listed')
            ]
            if not fallback.empty:
                factor = float(fallback['Factor'].iloc[0])
                logger.info(f"Base driver factor fallback for {coverage}: {factor}")
                return factor

        logger.warning(f"No base driver factor found for {coverage}, using default 1.0")
        return 1.0

    def get_all_driver_factors(self, marital_status: str, years_licensed: int, assigned_driver: str) -> dict:
        """Gets base driver factors for all coverages."""
        if self.base_driver_factors is None:
            self.initialize()
            
        try:
            # Get factors for each coverage
            coverages = ['BIPD', 'COLL', 'COMP', 'MPC', 'UM']
            factors = {}
            for coverage in coverages:
                # Create a mock driver object
                from models.models import Driver
                driver = Driver(
                    driver_id='temp',
                    years_licensed=years_licensed,
                    percentage_use=100.0,
                    assigned_driver=(assigned_driver == 'Yes'),
                    age=35,
                    marital_status=marital_status,
                    violations=[],
                    safety_record_level=0
                )
                
                # Handle UM coverage specially - map to U for table lookup
                if coverage == 'UM':
                    factor = self.get_base_driver_factor('U', driver)
                else:
                    factor = self.get_base_driver_factor(coverage, driver)
                
                # Map UM to U for frontend compatibility
                key = 'U' if coverage == 'UM' else coverage
                factors[key] = factor
            
            result = {
                'marital_status': marital_status,
                'years_licensed': years_licensed,
                'assigned_driver': assigned_driver,
                'factors': factors,
                'source_table': 'base_driver_factors'
            }
            
            logger.info(f"Driver factors: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error getting driver factors: {e}")
            return {
                'marital_status': marital_status,
                'years_licensed': years_licensed,
                'assigned_driver': assigned_driver,
                'factors': {'BIPD': 1.0, 'COLL': 1.0, 'COMP': 1.0, 'MPC': 1.0, 'U': 1.0},
                'source_table': 'base_driver_factors'
            }


    def get_years_licensed_factor(self, coverage: str, driver: Driver) -> float:
        """Gets the years licensed adjustment factor."""
        if self.years_licensed_factors is None:
            self.initialize()
            
        assigned_driver_str = 'Yes' if driver.assigned_driver else 'No'
        
        # Handle coverage name mapping (UM -> U in years licensed factors table)
        coverage_key = coverage
        if coverage == 'UM':
            coverage_key = 'U'
        
        # Find matching row in years licensed factors table
        match = self.years_licensed_factors[
            (self.years_licensed_factors['Coverage'] == coverage_key) &
            (self.years_licensed_factors['Assigned Driver'] == assigned_driver_str)
        ]
        
        if not match.empty:
            # Determine which range the driver's years licensed falls into
            years_licensed = driver.years_licensed
            for _, row in match.iterrows():
                years_range = row['Years Licensed']
                if '–' in years_range:
                    min_years, max_years = map(int, years_range.split(' – '))
                    if min_years <= years_licensed <= max_years:
                        factor = float(row['Factor'])
                        logger.info(f"Years licensed factor for {coverage}: {factor}")
                        return factor
                elif 'All Not Listed' in years_range:
                    factor = float(row['Factor'])
                    logger.info(f"Years licensed factor for {coverage}: {factor}")
                    return factor
        
        logger.warning(f"No years licensed factor found for {coverage}, using default 1.0")
        return 1.0

    def get_all_years_licensed_factors(self, years_licensed: int, assigned_driver: str) -> dict:
        """Gets years licensed factors for all coverages."""
        if self.years_licensed_factors is None:
            self.initialize()
            
        try:
            # Get factors for each coverage
            coverages = ['BIPD', 'COLL', 'COMP', 'MPC', 'UM']
            factors = {}
            for coverage in coverages:
                # Create a mock driver object
                from models.models import Driver
                driver = Driver(
                    driver_id='temp',
                    years_licensed=years_licensed,
                    percentage_use=100.0,
                    assigned_driver=(assigned_driver == 'Yes'),
                    age=35,
                    marital_status='M',
                    violations=[],
                    safety_record_level=0
                )
                
                # Handle UM coverage specially - map to U for table lookup
                if coverage == 'UM':
                    factor = self.get_years_licensed_factor('U', driver)
                else:
                    factor = self.get_years_licensed_factor(coverage, driver)
                
                # Map UM to U for frontend compatibility
                key = 'U' if coverage == 'UM' else coverage
                factors[key] = factor
            
            result = {
                'years_licensed': years_licensed,
                'assigned_driver': assigned_driver,
                'factors': factors,
                'source_table': 'years_liscensed_key'
            }
            
            logger.info(f"Years licensed factors: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error getting years licensed factors: {e}")
            return {
                'years_licensed': years_licensed,
                'assigned_driver': assigned_driver,
                'factors': {'BIPD': 1.0, 'COLL': 1.0, 'COMP': 1.0, 'MPC': 1.0, 'U': 1.0},
                'source_table': 'years_liscensed_key'
            }


    def get_percentage_use_factor(self, coverage: str, driver: Driver) -> float:
        """Gets the percentage use by driver factor."""
        if self.percentage_use_factors is None:
            self.initialize()
            
        # Determine the key based on driver percentage use and assignment
        if driver.assigned_driver:
            if driver.percentage_use == 100:
                key = 'Yes, No'  # Yes assigned, No other primary driver
            else:
                key = 'Yes, Yes'  # Yes assigned, Yes other primary driver
        else:
            key = 'All Not Specifically Listed, Yes'  # Not assigned driver
        
        try:
            factor = self.percentage_use_factors.loc[key, coverage]
            logger.info(f"Percentage use factor for {coverage}: {factor}")
            return float(factor)
        except (KeyError, IndexError):
            logger.warning(f"No percentage use factor found for {coverage}, using default 1.0")
            return 1.0

    def get_all_percentage_use_factors(self, percentage_use: float, assigned_driver: bool) -> dict:
        """Gets percentage use factors for all coverages."""
        if self.percentage_use_factors is None:
            self.initialize()
            
        try:
            # Determine the key based on driver percentage use and assignment
            if assigned_driver:
                if percentage_use == 100:
                    key = 'Yes, No'  # Yes assigned, No other primary driver
                else:
                    key = 'Yes, Yes'  # Yes assigned, Yes other primary driver
            else:
                key = 'All Not Specifically Listed, Yes'  # Not assigned driver
            
            # Get factors for each coverage
            coverages = ['BIPD', 'COLL', 'COMP', 'MPC', 'UM']
            factors = {}
            for coverage in coverages:
                try:
                    # Handle UM coverage specially - map to U for table lookup
                    table_coverage = 'U' if coverage == 'UM' else coverage
                    factor = self.percentage_use_factors.loc[key, table_coverage]
                    # Map UM to U for frontend compatibility
                    key_name = 'U' if coverage == 'UM' else coverage
                    factors[key_name] = float(factor)
                except (KeyError, IndexError):
                    key_name = 'U' if coverage == 'UM' else coverage
                    factors[key_name] = 1.0
            
            result = {
                'percentage_use': percentage_use,
                'assigned_driver': assigned_driver,
                'lookup_key': key,
                'factors': factors,
                'source_table': 'percentage_use_by_driver'
            }
            
            logger.info(f"Percentage use factors: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error getting percentage use factors: {e}")
            return {
                'percentage_use': percentage_use,
                'assigned_driver': assigned_driver,
                'factors': {'BIPD': 1.0, 'COLL': 1.0, 'COMP': 1.0, 'MPC': 1.0, 'U': 1.0},
                'source_table': 'percentage_use_by_driver'
            }


    def get_safety_record_factor(self, coverage: str, safety_level: int) -> float:
        """Gets the driving safety record factor based on safety record level."""
        if self.driving_safety_record_factors is None:
            self.initialize()
            
        try:
            # Map coverage names to column names in the safety record table
            column_mapping = {
                'BIPD': 'bipd_factor',
                'COLL': 'coll_factor', 
                'COMP': 'comp_factor',
                'MPC': 'mpc_factor',
                'UM': 'um_factor',
                'U': 'um_factor'  # Handle legacy 'U' mapping
            }
            
            column_name = column_mapping.get(coverage, coverage.lower() + '_factor')
            factor = self.driving_safety_record_factors.loc[safety_level, column_name]
            logger.info(f"Safety record factor for {coverage} at level {safety_level}: {factor}")
            return float(factor)
        except (KeyError, IndexError):
            logger.warning(f"No safety record factor found for {coverage} at level {safety_level}, using default 1.0")
            return 1.0

    def get_all_safety_factors(self, safety_level: int) -> dict:
        """Gets safety record factors for all coverages at a given safety level."""
        if self.driving_safety_record_factors is None:
            self.initialize()
            
        try:
            # Get factors for each coverage
            coverages = ['BIPD', 'COLL', 'COMP', 'MPC', 'UM']
            factors = {}
            for coverage in coverages:
                factor = self.get_safety_record_factor(coverage, safety_level)
                # Map UM to U for frontend compatibility
                key = 'U' if coverage == 'UM' else coverage
                factors[key] = factor
            
            result = {
                'safety_record_level': safety_level,
                'factors': factors,
                'source_table': 'driving_safety_record_rating_plan'
            }
            
            logger.info(f"Safety factors for level {safety_level}: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error getting safety factors for level {safety_level}: {e}")
            return {
                'safety_record_level': safety_level,
                'factors': {'BIPD': 1.0, 'COLL': 1.0, 'COMP': 1.0, 'MPC': 1.0, 'U': 1.0},
                'source_table': 'driving_safety_record_rating_plan'
            }


    def get_single_automobile_factor(self, coverage: str, usage: Usage) -> float:
        """Gets the single automobile factor."""
        if self.single_auto_factors is None:
            self.initialize()
            
        if not usage.single_automobile:
            return 1.0
        
        try:
            # Map coverage names to CSV column values
            coverage_mapping = {
                'BIPD': 'BIPD',
                'COLL': 'COLL', 
                'COMP': 'COMP',
                'MPC': 'MPC',
                'UM': 'U'  # UM coverage uses 'U' in the CSV
            }
            
            # Get the correct coverage value for lookup
            coverage_value = coverage_mapping.get(coverage, coverage)
            
            factor = self.single_auto_factors.loc[self.single_auto_factors['coverage'] == coverage_value, 'single_automobile_factor'].iloc[0]
            logger.info(f"Single auto factor for {coverage} (mapped to {coverage_value}): {factor}")
            return float(factor)
        except (KeyError, IndexError):
            logger.warning(f"No single auto factor found for {coverage}, using default 1.0")
            return 1.0

    def get_all_single_auto_factors(self, single_auto: bool) -> dict:
        """Gets single auto factors for all coverages."""
        if self.single_auto_factors is None:
            self.initialize()
            
        try:
            # Create a mock usage object
            from models.models import Usage
            usage = Usage(
                annual_mileage=10000,
                type='Pleasure / Work / School',
                single_automobile=single_auto
            )
            
            # Get factors for each coverage
            coverages = ['BIPD', 'COLL', 'COMP', 'MPC', 'UM']
            factors = {}
            for coverage in coverages:
                factor = self.get_single_automobile_factor(coverage, usage)
                # Map UM to U for frontend compatibility
                key = 'U' if coverage == 'UM' else coverage
                factors[key] = factor
            
            result = {
                'single_automobile': single_auto,
                'factors': factors,
                'source_table': 'single_auto_factor'
            }
            
            logger.info(f"Single auto factors: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error getting single auto factors: {e}")
            return {
                'single_automobile': single_auto,
                'factors': {'BIPD': 1.0, 'COLL': 1.0, 'COMP': 1.0, 'MPC': 1.0, 'U': 1.0},
                'source_table': 'single_auto_factor'
            }


    def get_annual_mileage_factor(self, coverage: str, usage: Usage) -> float:
        """Gets the annual mileage factor."""
        if self.annual_mileage_factors is None:
            self.initialize()
            
        try:
            annual_mileage = usage.annual_mileage
            
            # Map coverage names to CSV column names
            coverage_column_map = {
                'BIPD': 'BIPD',
                'COLL': 'COLL', 
                'COMP': 'COMP',
                'MPC': 'MPC',
                'UM': 'U'  # UM coverage uses 'U' column in the CSV
            }
            
            # Get the correct column name for this coverage
            column_name = coverage_column_map.get(coverage, coverage)
            
            # Find the appropriate mileage band
            for _, row in self.annual_mileage_factors.iterrows():
                lower_bound = row['lower_bound']
                
                # Handle the special case for the last row (33950+)
                if 'Annual Mileage' in row and '+' in str(row['Annual Mileage']):
                    # This is the highest mileage band
                    if annual_mileage >= lower_bound:
                        factor = float(row[column_name])
                        logger.info(f"Annual mileage factor for {coverage} (column {column_name}): {factor}")
                        return factor
                else:
                    # For other rows, find the next row's lower bound to determine upper bound
                    next_row_idx = self.annual_mileage_factors.index.get_loc(row.name) + 1
                    if next_row_idx < len(self.annual_mileage_factors):
                        next_row = self.annual_mileage_factors.iloc[next_row_idx]
                        upper_bound = next_row['lower_bound']
                        if lower_bound <= annual_mileage < upper_bound:
                            factor = float(row[column_name])
                            logger.info(f"Annual mileage factor for {coverage} (column {column_name}): {factor}")
                            return factor
                    else:
                        # Last row (shouldn't happen with current logic)
                        if annual_mileage >= lower_bound:
                            factor = float(row[column_name])
                            logger.info(f"Annual mileage factor for {coverage} (column {column_name}): {factor}")
                            return factor
            
            logger.warning(f"No annual mileage factor found for {coverage}, using default 1.0")
            return 1.0
            
        except Exception as e:
            logger.error(f"Error getting annual mileage factor for {coverage}: {e}")
            return 1.0

    def get_all_annual_mileage_factors(self, annual_mileage: int) -> dict:
        """Gets annual mileage factors for all coverages."""
        if self.annual_mileage_factors is None:
            self.initialize()
            
        try:
            # Create a mock usage object
            from models.models import Usage
            usage = Usage(
                annual_mileage=annual_mileage,
                type='Pleasure / Work / School',
                single_automobile=True
            )
            
            # Get factors for each coverage
            coverages = ['BIPD', 'COLL', 'COMP', 'MPC', 'UM']
            factors = {}
            for coverage in coverages:
                factor = self.get_annual_mileage_factor(coverage, usage)
                # Map UM to U for frontend compatibility
                key = 'U' if coverage == 'UM' else coverage
                factors[key] = factor
            
            result = {
                'annual_mileage': annual_mileage,
                'factors': factors,
                'source_table': 'annual_mileage_factor'
            }
            
            logger.info(f"Annual mileage factors: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error getting annual mileage factors: {e}")
            return {
                'annual_mileage': annual_mileage,
                'factors': {'BIPD': 1.0, 'COLL': 1.0, 'COMP': 1.0, 'MPC': 1.0, 'U': 1.0},
                'source_table': 'annual_mileage_factor'
            }

    def get_usage_type_factor(self, coverage: str, usage: Usage) -> float:
        """Gets the usage type factor."""
        if self.usage_type_factors is None:
            self.initialize()
            
        try:
            usage_type = usage.type
            
            # Map coverage names to CSV column names
            coverage_column_map = {
                'BIPD': 'bipd_factor',
                'COLL': 'coll_factor', 
                'COMP': 'comp_factor',
                'MPC': 'mpc_factor',
                'UM': 'um_factor'  # UM coverage uses 'um_factor' column in the CSV
            }
            
            # Get the correct column name for this coverage
            column_name = coverage_column_map.get(coverage, coverage.lower() + '_factor')
            
            # Since the DataFrame is indexed by automobile_use, we can directly access it
            if usage_type in self.usage_type_factors.index:
                row = self.usage_type_factors.loc[usage_type]
                # Get the factor for this coverage using the mapped column name
                if column_name in row:
                    factor = float(row[column_name])
                    logger.info(f"Usage type factor for {coverage} (column {column_name}): {factor}")
                    return factor
                else:
                    logger.warning(f"Column {column_name} not found for usage type {usage_type}")
                    return 1.0
            
            logger.warning(f"No usage type factor found for {coverage}, using default 1.0")
            return 1.0
            
        except Exception as e:
            logger.error(f"Error getting usage type factor for {coverage}: {e}")
            return 1.0

    def get_all_usage_type_factors(self, usage_type: str) -> dict:
        """Gets usage type factors for all coverages."""
        if self.usage_type_factors is None:
            self.initialize()
            
        try:
            # Create a mock usage object
            from models.models import Usage
            usage = Usage(
                annual_mileage=10000,
                type=usage_type,
                single_automobile=True
            )
            
            # Get factors for each coverage
            coverages = ['BIPD', 'COLL', 'COMP', 'MPC', 'UM']
            factors = {}
            for coverage in coverages:
                factor = self.get_usage_type_factor(coverage, usage)
                # Map UM to U for frontend compatibility
                key = 'U' if coverage == 'UM' else coverage
                factors[key] = factor
            
            result = {
                'usage_type': usage_type,
                'factors': factors,
                'source_table': 'auto_usage_type'
            }
            
            logger.info(f"Usage type factors: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error getting usage type factors: {e}")
            return {
                'usage_type': usage_type,
                'factors': {'BIPD': 1.0, 'COLL': 1.0, 'COMP': 1.0, 'MPC': 1.0, 'U': 1.0},
                'source_table': 'auto_usage_type'
            }

    def calculate_discount_factors(self, coverage: str, discounts: Discounts) -> Dict:
        """
        Calculates discount factors for a specific coverage.
        This method delegates to the DiscountService for discount calculations.
        
        Args:
            coverage: The coverage type (e.g., 'BIPD', 'COLL', etc.)
            discounts: Discounts object containing all discount information
            
        Returns:
            Dictionary with discount factors for the coverage
        """
        try:
            # Use the existing DiscountService to calculate discount factors
            # We need to create a mock special_factors object since the method signature requires it
            from models.models import SpecialFactors
            special_factors = SpecialFactors(
                federal_employee=getattr(discounts, 'federal_employee', False),
                transportation_network_company=getattr(discounts, 'transportation_network_company', False),
                transportation_of_friends=getattr(discounts, 'transportation_of_friends', False)
            )
            
            # Get discount factors for this coverage
            discount_results = self.discount_service.calculate_discount_factors(
                discounts, special_factors, [coverage]
            )
            
            # Extract the breakdown for this coverage
            coverage_discounts = discount_results.get(coverage, {}).get('breakdown', {})
            
            logger.info(f"Discount factors for {coverage}: {coverage_discounts}")
            return coverage_discounts
            
        except Exception as e:
            logger.error(f"Error calculating discount factors for {coverage}: {e}")
            return {}
