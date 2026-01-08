import pandas as pd
import logging
from typing import Dict, List, Optional
from app.utils.data_loader import DataLoader
from app.models.models import Driver, Usage, Discounts
from app.services.calculations.discount_service import DiscountService
from app.models.models import SpecialFactors
from app.models.models import Usage
from app.models.models import Driver
BASE_DRIVER_FACTOR_COLLECTION="base-driver-factors"

logger = logging.getLogger(__name__)

# years licensed ranges are in MONTHS (0–83)
YEARS_LICENSED_FACTORS = {
    "BIPD": {
        True: [
            ((0, 48), 1.0),
            ((49, 83), 0.873),
        ],
        False: [
            (None, 0.873),  # All Not Listed
        ],
    },
    "COLL": {
        True: [
            ((0, 48), 1.0),
            ((49, 83), 0.943),
        ],
        False: [
            (None, 0.943),
        ],
    },
    "COMP": {
        True: [
            ((0, 48), 1.0),
            ((49, 83), 0.95),
        ],
        False: [
            (None, 0.95),
        ],
    },
    "MPC": {
        True: [
            ((0, 48), 1.0),
            ((49, 83), 0.769),
        ],
        False: [
            (None, 0.769),
        ],
    },
    "U": {
        True: [
            ((0, 48), 1.0),
            ((49, 83), 0.788),
        ],
        False: [
            (None, 0.788),
        ],
    },
}

PERCENTAGE_USE_FACTORS = {
    "No, No": {
        "BIPD": 1.0,
        "COLL": 1.0,
        "COMP": 1.0,
        "MPC": 1.0,
        "UM": 1.0,
    },
    "Yes, No": {
        "BIPD": 0.817,
        "COLL": 0.816,
        "COMP": 0.753,
        "MPC": 0.792,
        "UM": 0.871,
    },
    "All Not Specifically Listed, Yes": {
        "BIPD": 0.718,
        "COLL": 0.749,
        "COMP": 0.714,
        "MPC": 0.713,
        "UM": 0.851,
    },
}

SAFETY_RECORD_FACTORS = {
    0:  {"BIPD": 0.506, "COLL": 0.53,  "COMP": 0.641, "MPC": 0.573, "UM": 0.623},
    1:  {"BIPD": 0.65,  "COLL": 0.633, "COMP": 0.664, "MPC": 0.686, "UM": 0.678},
    2:  {"BIPD": 0.667, "COLL": 0.672, "COMP": 0.684, "MPC": 0.714, "UM": 0.737},
    3:  {"BIPD": 0.73,  "COLL": 0.733, "COMP": 0.775, "MPC": 0.774, "UM": 0.783},
    4:  {"BIPD": 0.769, "COLL": 0.777, "COMP": 0.788, "MPC": 0.809, "UM": 0.837},
    5:  {"BIPD": 0.803, "COLL": 0.822, "COMP": 0.795, "MPC": 0.818, "UM": 0.845},
    6:  {"BIPD": 0.843, "COLL": 0.84,  "COMP": 0.802, "MPC": 0.876, "UM": 0.868},
    7:  {"BIPD": 0.904, "COLL": 0.897, "COMP": 0.906, "MPC": 0.906, "UM": 0.909},
    8:  {"BIPD": 1.0,   "COLL": 1.0,   "COMP": 1.0,   "MPC": 1.0,   "UM": 1.0},
    9:  {"BIPD": 1.019, "COLL": 1.012, "COMP": 1.1,   "MPC": 1.169, "UM": 1.267},
    10: {"BIPD": 1.124, "COLL": 1.044, "COMP": 1.233, "MPC": 1.223, "UM": 1.278},
    11: {"BIPD": 1.152, "COLL": 1.156, "COMP": 1.367, "MPC": 1.266, "UM": 1.363},
    12: {"BIPD": 1.267, "COLL": 1.269, "COMP": 1.5,   "MPC": 1.38,  "UM": 1.486},
    13: {"BIPD": 1.427, "COLL": 1.39,  "COMP": 1.733, "MPC": 1.617, "UM": 1.688},
    14: {"BIPD": 1.461, "COLL": 1.439, "COMP": 1.867, "MPC": 1.754, "UM": 1.852},
    15: {"BIPD": 1.588, "COLL": 1.646, "COMP": 2.033, "MPC": 1.906, "UM": 2.064},
    16: {"BIPD": 1.662, "COLL": 1.746, "COMP": 2.2,   "MPC": 2.064, "UM": 2.164},
    17: {"BIPD": 1.866, "COLL": 1.861, "COMP": 2.367, "MPC": 2.273, "UM": 2.362},
    18: {"BIPD": 2.135, "COLL": 2.125, "COMP": 2.49,  "MPC": 2.364, "UM": 2.541},
    19: {"BIPD": 2.233, "COLL": 2.156, "COMP": 2.596, "MPC": 2.51,  "UM": 2.602},
    20: {"BIPD": 2.247, "COLL": 2.282, "COMP": 2.881, "MPC": 2.638, "UM": 2.838},
    21: {"BIPD": 2.4,   "COLL": 2.432, "COMP": 3.061, "MPC": 2.822, "UM": 3.026},
    22: {"BIPD": 2.444, "COLL": 2.625, "COMP": 3.083, "MPC": 2.877, "UM": 3.121},
    23: {"BIPD": 2.628, "COLL": 2.702, "COMP": 3.266, "MPC": 3.152, "UM": 3.515},
    24: {"BIPD": 2.785, "COLL": 2.83,  "COMP": 3.418, "MPC": 3.494, "UM": 3.64},
    25: {"BIPD": 2.854, "COLL": 3.072, "COMP": 3.767, "MPC": 3.533, "UM": 3.724},
    26: {"BIPD": 2.997, "COLL": 3.209, "COMP": 3.998, "MPC": 3.64,  "UM": 3.855},
    27: {"BIPD": 3.025, "COLL": 3.248, "COMP": 4.0,   "MPC": 3.562, "UM": 4.074},
    28: {"BIPD": 3.09,  "COLL": 3.425, "COMP": 4.127, "MPC": 3.892, "UM": 4.146},
    29: {"BIPD": 3.146, "COLL": 3.449, "COMP": 4.162, "MPC": 4.367, "UM": 4.23},
    30: {"BIPD": 3.458, "COLL": 3.712, "COMP": 4.899, "MPC": 4.38,  "UM": 4.687},
}

SINGLE_AUTO_FACTOR_LOOKUP = {
    'BIPD': 1.259,
    'COLL': 1.255,
    'COMP': 1.081,
    'MPC': 1.263,
    'U': 1.319,
}

ANNUAL_MILEAGE_FACTOR_LOOKUP = {
    0:     {'BIPD': 0.617, 'COLL': 0.581, 'COMP': 0.652, 'MPC': 0.645, 'U': 0.667},
    1450:  {'BIPD': 0.675, 'COLL': 0.697, 'COMP': 0.662, 'MPC': 0.674, 'U': 0.691},
    2450:  {'BIPD': 0.743, 'COLL': 0.742, 'COMP': 0.700, 'MPC': 0.709, 'U': 0.733},
    3450:  {'BIPD': 0.757, 'COLL': 0.780, 'COMP': 0.748, 'MPC': 0.723, 'U': 0.775},
    4450:  {'BIPD': 0.816, 'COLL': 0.803, 'COMP': 0.767, 'MPC': 0.779, 'U': 0.795},
    5450:  {'BIPD': 0.859, 'COLL': 0.852, 'COMP': 0.815, 'MPC': 0.783, 'U': 0.815},
    6450:  {'BIPD': 0.898, 'COLL': 0.910, 'COMP': 0.853, 'MPC': 0.830, 'U': 0.892},
    7450:  {'BIPD': 0.941, 'COLL': 0.951, 'COMP': 0.890, 'MPC': 0.913, 'U': 0.920},
    8450:  {'BIPD': 0.957, 'COLL': 0.969, 'COMP': 0.920, 'MPC': 0.982, 'U': 0.989},
    9450:  {'BIPD': 0.976, 'COLL': 0.996, 'COMP': 0.940, 'MPC': 0.989, 'U': 0.996},
    10450: {'BIPD': 1.000, 'COLL': 1.000, 'COMP': 1.000, 'MPC': 1.000, 'U': 1.000},
    11450: {'BIPD': 1.002, 'COLL': 1.002, 'COMP': 1.007, 'MPC': 1.014, 'U': 1.011},
    12450: {'BIPD': 1.006, 'COLL': 1.004, 'COMP': 1.034, 'MPC': 1.021, 'U': 1.026},
    13450: {'BIPD': 1.009, 'COLL': 1.010, 'COMP': 1.045, 'MPC': 1.028, 'U': 1.049},
    14450: {'BIPD': 1.028, 'COLL': 1.021, 'COMP': 1.069, 'MPC': 1.035, 'U': 1.062},
    15450: {'BIPD': 1.047, 'COLL': 1.029, 'COMP': 1.074, 'MPC': 1.043, 'U': 1.070},
    16450: {'BIPD': 1.069, 'COLL': 1.054, 'COMP': 1.091, 'MPC': 1.050, 'U': 1.077},
    17450: {'BIPD': 1.112, 'COLL': 1.058, 'COMP': 1.099, 'MPC': 1.057, 'U': 1.087},
    18450: {'BIPD': 1.122, 'COLL': 1.077, 'COMP': 1.155, 'MPC': 1.071, 'U': 1.096},
    19450: {'BIPD': 1.155, 'COLL': 1.104, 'COMP': 1.199, 'MPC': 1.092, 'U': 1.099},
    20450: {'BIPD': 1.168, 'COLL': 1.114, 'COMP': 1.208, 'MPC': 1.106, 'U': 1.103},
    21450: {'BIPD': 1.178, 'COLL': 1.124, 'COMP': 1.218, 'MPC': 1.121, 'U': 1.151},
    23950: {'BIPD': 1.187, 'COLL': 1.160, 'COMP': 1.285, 'MPC': 1.135, 'U': 1.166},
    26450: {'BIPD': 1.194, 'COLL': 1.206, 'COMP': 1.323, 'MPC': 1.149, 'U': 1.180},
    28950: {'BIPD': 1.215, 'COLL': 1.234, 'COMP': 1.333, 'MPC': 1.156, 'U': 1.195},
    31450: {'BIPD': 1.263, 'COLL': 1.271, 'COMP': 1.342, 'MPC': 1.160, 'U': 1.210},
    33950: {'BIPD': 1.355, 'COLL': 1.322, 'COMP': 1.362, 'MPC': 1.184, 'U': 1.224},
}

USAGE_TYPE_FACTOR_LOOKUP = {
    'Farm': {
        'usage_type_code': 3,
        'factors': {
            'bipd_factor': 0.865,
            'coll_factor': 0.848,
            'comp_factor': 0.860,
            'mpc_factor': 0.907,
            'um_factor':  1.056,
        },
    },
    'Pleasure / Work / School': {
        'usage_type_code': 1,
        'factors': {
            'bipd_factor': 1.000,
            'coll_factor': 1.000,
            'comp_factor': 1.000,
            'mpc_factor': 1.000,
            'um_factor':    1.000,
        },
    },
    'Business': {
        'usage_type_code': 2,
        'factors': {
            'bipd_factor': 1.257,
            'coll_factor': 0.918,
            'comp_factor': 0.936,
            'mpc_factor': 0.917,
            'um_factor':    0.975,
        },
    },
}





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
        # DEPRECATED self.base_driver_factors = self.data_loader.load_base_driver_factors()
        # DEPRECATED self.years_licensed_factors = self.data_loader.load_years_licensed_key()
        # DEPRECATED self.percentage_use_factors = self.data_loader.load_percentage_use_by_driver()
        # DEPRECATED self.driving_safety_record_factors = self.data_loader.load_driving_safety_record_rating_plan()
        # DEPRECATED self.single_auto_factors = self.data_loader.load_single_auto_factors()
        # DEPRECATED (?) self.annual_mileage_factors = self.data_loader.load_annual_mileage_factors()
        # DEPRECATED self.usage_type_factors = self.data_loader.load_usage_type_factors()
        
        logger.info("DriverFactorLookupService initialized")
        
    def get_base_driver_factor(self, coverage: str, driver: Driver) -> float:
        """Gets the base driver factor from the rating tables."""
        # Deprecated
        # if self.base_driver_factors is None:
        #     self.initialize()
            
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
        match_query = {"Coverage": coverage_key, "Assigned Driver": assigned_driver_str, "Marital Status": lookup_marital_status, "Years Driving": lookup_years_driving}
        match_result = self.data_loader.storage_service.find(match_query,BASE_DRIVER_FACTOR_COLLECTION)
        match = match_result[0] if match_result else None    
        # Deprecated match from CSV
        # match = self.base_driver_factors[
        #     (self.base_driver_factors['Coverage'] == coverage_key) &
        #     (self.base_driver_factors['Assigned Driver'] == assigned_driver_str) &
        #     (self.base_driver_factors['Marital Status'] == lookup_marital_status) &
        #     (self.base_driver_factors['Years Driving'] == lookup_years_driving)
        # ]

        if match:
            factor = float(match['Factor'])
            logger.info(f"Base driver factor for {coverage}: {factor}")
            return factor
        else:
            # Fallback logic - use the same format as the working old service
            fallback_query = {"Coverage": coverage_key, "Assigned Driver": assigned_driver_str, "Marital Status": 'All Not\n Specifically\n Listed', "Years Driving": 'All Not\n Specifically\n Listed'}
            fallback_result = self.data_loader.storage_service.find(fallback_query, BASE_DRIVER_FACTOR_COLLECTION)
            fallback = fallback_result[0] if fallback_result else None    

            #  Deprecated fallback from CSV

            # fallback = self.base_driver_factors[
            #     (self.base_driver_factors['Coverage'] == coverage_key) &
            #     (self.base_driver_factors['Assigned Driver'] == assigned_driver_str) &
            #     (self.base_driver_factors['Marital Status'] == 'All Not\n Specifically\n Listed') &
            #     (self.base_driver_factors['Years Driving'] == 'All Not\n Specifically\n Listed')
            # ]

            if fallback:
                factor = float(fallback['Factor'])
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
        # if self.years_licensed_factors is None:
        #     self.initialize()
            
        assigned_driver_str = 'Yes' if driver.assigned_driver else 'No'
        
        # Handle coverage name mapping (UM -> U in years licensed factors table)
        coverage_key = coverage
        if coverage == 'UM':
            coverage_key = 'U'
        assigned = bool(driver.assigned_driver)
        years_licensed = driver.years_licensed

        coverage_table = YEARS_LICENSED_FACTORS.get(coverage_key)
        if not coverage_table:
            logger.warning(f"No years licensed table for coverage {coverage_key}")
            return 1.0

        rules = coverage_table.get(assigned)
        if not rules:
            logger.warning(
                f"No years licensed rules for coverage={coverage_key}, assigned={assigned}"
            )
            return 1.0

        for year_range, factor in rules:
            if year_range is None:
                # "All Not Listed"
                logger.info(f"Years licensed factor for {coverage}: {factor}")
                return factor

            min_years, max_years = year_range
            if min_years <= years_licensed <= max_years:
                logger.info(f"Years licensed factor for {coverage}: {factor}")
                return factor

        logger.warning(f"No years licensed factor matched for {coverage}, using default 1.0")
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
        # if self.percentage_use_factors is None:
        #     self.initialize()
            
        # Determine the key based on driver percentage use and assignment
        if driver.assigned_driver:
            if driver.percentage_use == 100:
                key = 'Yes, No'  # Yes assigned, No other primary driver
            else:
                key = 'Yes, Yes'  # Yes assigned, Yes other primary driver
        else:
            key = 'All Not Specifically Listed, Yes'  # Not assigned driver
        
        try:
            factor = (
                PERCENTAGE_USE_FACTORS
                .get(key, {})
                .get(coverage)
            )
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
        # if self.driving_safety_record_factors is None:
        #     self.initialize()
            
        try:
            # # Map coverage names to column names in the safety record table
            # column_mapping = {
            #     'BIPD': 'bipd_factor',
            #     'COLL': 'coll_factor', 
            #     'COMP': 'comp_factor',
            #     'MPC': 'mpc_factor',
            #     'UM': 'um_factor',
            #     'U': 'um_factor'  # Handle legacy 'U' mapping
            # }
            
            # column_name = column_mapping.get(coverage, coverage.lower() + '_factor')
            factor = SAFETY_RECORD_FACTORS.get(safety_level, {}).get(coverage)
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
        # if self.single_auto_factors is None:
        #     self.initialize()
            
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
            factor = SINGLE_AUTO_FACTOR_LOOKUP.get(coverage_value)

            # DEPRECATED factor = self.single_auto_factors.loc[self.single_auto_factors['coverage'] == coverage_value, 'single_automobile_factor'].iloc[0]
            if factor is None:
                raise ValueError(f"No single auto factor found for coverage '{coverage_value}'")

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
        # if self.annual_mileage_factors is None:
        #     self.initialize()
            
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
            lower_bounds = sorted(ANNUAL_MILEAGE_FACTOR_LOOKUP.keys())
            # improvement :make this O(1) not O(n) is this our first time coding????
            for i, lower_bound in enumerate(lower_bounds):
                # Last band: open-ended (33950+)
                if i == len(lower_bounds) - 1:
                    if annual_mileage >= lower_bound:
                        try:
                            factor = ANNUAL_MILEAGE_FACTOR_LOOKUP[lower_bound][column_name]
                        except KeyError:
                            raise ValueError(
                                f"No annual mileage factor for lower_bound={lower_bound}, coverage={column_name}"
                            )

                        logger.info(
                            f"Annual mileage factor for {coverage} (lower_bound {lower_bound}+): {factor}"
                        )
                        return float(factor)

                # Normal band
                next_lower_bound = lower_bounds[i + 1]
                if lower_bound <= annual_mileage < next_lower_bound:
                    try:
                        factor = ANNUAL_MILEAGE_FACTOR_LOOKUP[lower_bound][column_name]
                    except KeyError:
                        raise ValueError(
                            f"No annual mileage factor for lower_bound={lower_bound}, coverage={column_name}"
                        )

                    logger.info(
                        f"Annual mileage factor for {coverage} "
                        f"(range {lower_bound}–{next_lower_bound - 1}): {factor}"
                    )
                    return float(factor)

            # Should never happen if data is correct
            raise ValueError(f"No annual mileage band found for annual_mileage={annual_mileage}")
            return 1.0
            # DEPRECATED
            # # Find the appropriate mileage band
            # for _, row in self.annual_mileage_factors.iterrows():
            #     lower_bound = row['lower_bound']
                
            #     # Handle the special case for the last row (33950+)
            #     if 'Annual Mileage' in row and '+' in str(row['Annual Mileage']):
            #         # This is the highest mileage band
            #         if annual_mileage >= lower_bound:
            #             factor = float(row[column_name])
            #             logger.info(f"Annual mileage factor for {coverage} (column {column_name}): {factor}")
            #             return factor
            #     else:
            #         # For other rows, find the next row's lower bound to determine upper bound
            #         next_row_idx = self.annual_mileage_factors.index.get_loc(row.name) + 1
            #         if next_row_idx < len(self.annual_mileage_factors):
            #             next_row = self.annual_mileage_factors.iloc[next_row_idx]
            #             upper_bound = next_row['lower_bound']
            #             if lower_bound <= annual_mileage < upper_bound:
            #                 factor = float(row[column_name])
            #                 logger.info(f"Annual mileage factor for {coverage} (column {column_name}): {factor}")
            #                 return factor
            #         else:
            #             # Last row (shouldn't happen with current logic)
            #             if annual_mileage >= lower_bound:
            #                 factor = float(row[column_name])
            #                 logger.info(f"Annual mileage factor for {coverage} (column {column_name}): {factor}")
            #                 return factor
            
            # logger.warning(f"No annual mileage factor found for {coverage}, using default 1.0")
            # return 1.0
            
        except Exception as e:
            logger.error(f"Error getting annual mileage factor for {coverage}: {e}")
            return 1.0

    def get_all_annual_mileage_factors(self, annual_mileage: int) -> dict:
        """Gets annual mileage factors for all coverages."""
        if self.annual_mileage_factors is None:
            self.initialize()
            
        try:
            # Create a mock usage object 
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
        # if self.usage_type_factors is None:
        #     self.initialize()
            
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
            usage_entry = USAGE_TYPE_FACTOR_LOOKUP.get(usage_type)
            
            if usage_entry is None:
                logger.warning(
                    f"No usage type factor found for {coverage}, using default 1.0"
                )
                return 1.0
            
            factor = usage_entry['factors'].get(column_name)
            
            if factor is None:
                logger.warning(
                    f"Coverage {column_name} not found for usage type {usage_type}"
                )
                return 1.0
            
            logger.info(
            f"Usage type factor for {coverage} "
            f"(usage_type={usage_type}, code={usage_entry['usage_type_code']}): "
            f"{factor}"
            )
            return float(factor)


            # DEPRECATED
            # # Since the DataFrame is indexed by automobile_use, we can directly access it
            # if usage_type in self.usage_type_factors.index:
            #     row = self.usage_type_factors.loc[usage_type]
            #     # Get the factor for this coverage using the mapped column name
            #     if column_name in row:
            #         factor = float(row[column_name])
            #         logger.info(f"Usage type factor for {coverage} (column {column_name}): {factor}")
            #         return factor
            #     else:
            #         logger.warning(f"Column {column_name} not found for usage type {usage_type}")
            #         return 1.0
            
            # logger.warning(f"No usage type factor found for {coverage}, using default 1.0")
            # return 1.0
            
        except Exception as e:
            logger.error(f"Error getting usage type factor for {coverage}: {e}")
            return 1.0

    def get_all_usage_type_factors(self, usage_type: str) -> dict:
        """Gets usage type factors for all coverages."""
        if self.usage_type_factors is None:
            self.initialize()
            
        try:
            # Create a mock usage object

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
