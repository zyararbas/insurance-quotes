import pandas as pd
import os
from functools import lru_cache
import logging
from app.services.storage_service import StorageService
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
VEHICLE_RATES = "vehicle-rates"
class DataLoader:
    """
    Handles loading and caching of insurance rating tables from CSV files.
    Provides efficient access to data using pandas and in-memory caching.
    """
    def __init__(self):
        # Assumes the 'Data' directory is at the app level.
        self.base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'Data', 'California', 'STATEFARM_CA_Insurance__tables'))
        logger.info(f"DataLoader initialized with base path: {self.base_path}")
        self.storage_service = StorageService()
        if not os.path.isdir(self.base_path):
            logger.warning(f"Data directory not found at expected path: {self.base_path}")

    @lru_cache(maxsize=128)
    def load_table(self, table_path: str) -> pd.DataFrame:
        """
        Loads a CSV table into a pandas DataFrame with LRU caching.
        This decorator automatically caches the results of this function.
        """
        full_path = os.path.join(self.base_path, table_path)
        try:
            logger.info(f"Loading table from: {full_path}")
            # Use the 'python' engine for more robust parsing of potentially tricky CSV files.
            df = pd.read_csv(full_path, skipinitialspace=True, engine='python')
            # Clean up column names (remove leading/trailing spaces)
            df.columns = df.columns.str.strip()
            return df
        except FileNotFoundError:
            logger.error(f"Failed to find table at {full_path}")
            raise
        except Exception as e:
            logger.error(f"Failed to load table {table_path}: {e}")
            raise

    def load_base_rates(self) -> dict:
        """Loads base rates and returns them as a {coverage: rate} dictionary."""
        df = self.load_table('base_factors/base_rates - Sheet1.csv')
        return pd.Series(df.base_rate.values, index=df.coverage).to_dict()

    def load_territory_factors(self) -> pd.DataFrame:
        """Loads territory factors, indexed by zip code."""
        df = self.load_table('base_factors/CA_zip_territory_factors - Sheet1.csv')
        df = df.set_index('zip')
        return df
    # Deprecated 
    def load_zip_territory_factors(self) -> pd.DataFrame:
        """Loads zip territory factors (alias for load_territory_factors)."""
        # return self.load_territory_factors()
        return

    def load_vehicle_ratings(self) -> pd.DataFrame:
        """Loads vehicle ratings groups."""
        # df = self.load_table('car_factors/vehicle_ratings_groups - Sheet1.csv')
        try:
            df = self.storage_service.get_collection_as_dataframe(VEHICLE_RATES)
            # Standardize column names to lowercase
            df.columns = df.columns.str.lower()

            # Create a standardized key for easy lookups, similar to the JS version
            df['lookup_key'] = df.apply(
                lambda row: self._create_vehicle_key(
                    row['year'], row['make'], row['model'],
                    row.get('series', ''), row.get('package', ''),
                    row.get('style', ''), row.get('engine', '')
                ),
                axis=1
            )
            return df.set_index('lookup_key')
        except Exception as e:
            logging.error(f"Vehicle rates could not be loaded {e}")
            raise Exception(e)
        

    def load_vehicle_ratings_groups(self) -> pd.DataFrame:
        """Loads vehicle ratings groups (alias for load_vehicle_ratings)."""
        return self.load_vehicle_ratings()

    def load_fallback_vehicle_ratings(self) -> pd.DataFrame:
        """Loads fallback vehicle ratings (by MSRP)."""
        data = [
            {'MSRP_range': '$0 - $2200', 'MSRP_min': 0, 'MSRP_max': 2200, 'GRG': 1, 'DRG': 1, 'VSD': 'C', 'LRG': 5},
            {'MSRP_range': '$2201 - $3000', 'MSRP_min': 2201, 'MSRP_max': 3000, 'GRG': 2, 'DRG': 2, 'VSD': 'C', 'LRG': 5},
            {'MSRP_range': '$3001 - $4000', 'MSRP_min': 3001, 'MSRP_max': 4000, 'GRG': 3, 'DRG': 3, 'VSD': 'C', 'LRG': 5},
            {'MSRP_range': '$4001 - $5000', 'MSRP_min': 4001, 'MSRP_max': 5000, 'GRG': 4, 'DRG': 4, 'VSD': 'C', 'LRG': 5},
            {'MSRP_range': '$5001 - $6000', 'MSRP_min': 5001, 'MSRP_max': 6000, 'GRG': 5, 'DRG': 5, 'VSD': 'C', 'LRG': 5},
            {'MSRP_range': '$6001 - $7000', 'MSRP_min': 6001, 'MSRP_max': 7000, 'GRG': 6, 'DRG': 6, 'VSD': 'C', 'LRG': 5},
            {'MSRP_range': '$7001 - $8000', 'MSRP_min': 7001, 'MSRP_max': 8000, 'GRG': 7, 'DRG': 7, 'VSD': 'C', 'LRG': 5},
            {'MSRP_range': '$8001 - $9000', 'MSRP_min': 8001, 'MSRP_max': 9000, 'GRG': 8, 'DRG': 8, 'VSD': 'C', 'LRG': 5},
            {'MSRP_range': '$9001 - $10000', 'MSRP_min': 9001, 'MSRP_max': 10000, 'GRG': 9, 'DRG': 9, 'VSD': 'C', 'LRG': 5},
            {'MSRP_range': '$10001 - $11000', 'MSRP_min': 10001, 'MSRP_max': 11000, 'GRG': 10, 'DRG': 10, 'VSD': 'C', 'LRG': 5},
            {'MSRP_range': '$11001 - $12000', 'MSRP_min': 11001, 'MSRP_max': 12000, 'GRG': 11, 'DRG': 11, 'VSD': 'C', 'LRG': 5},
            {'MSRP_range': '$12001 - $14000', 'MSRP_min': 12001, 'MSRP_max': 14000, 'GRG': 12, 'DRG': 12, 'VSD': 'C', 'LRG': 5},
            {'MSRP_range': '$14001 - $16000', 'MSRP_min': 14001, 'MSRP_max': 16000, 'GRG': 13, 'DRG': 13, 'VSD': 'C', 'LRG': 5},
            {'MSRP_range': '$16001 - $18000', 'MSRP_min': 16001, 'MSRP_max': 18000, 'GRG': 14, 'DRG': 14, 'VSD': 'C', 'LRG': 5},
            {'MSRP_range': '$18001 - $20000', 'MSRP_min': 18001, 'MSRP_max': 20000, 'GRG': 15, 'DRG': 15, 'VSD': 'C', 'LRG': 5},
            {'MSRP_range': '$20001 - $22000', 'MSRP_min': 20001, 'MSRP_max': 22000, 'GRG': 16, 'DRG': 16, 'VSD': 'C', 'LRG': 5},
            {'MSRP_range': '$22001 - $24000', 'MSRP_min': 22001, 'MSRP_max': 24000, 'GRG': 17, 'DRG': 17, 'VSD': 'C', 'LRG': 5},
            {'MSRP_range': '$24001 - $26000', 'MSRP_min': 24001, 'MSRP_max': 26000, 'GRG': 18, 'DRG': 18, 'VSD': 'C', 'LRG': 5},
            {'MSRP_range': '$26001 - $28000', 'MSRP_min': 26001, 'MSRP_max': 28000, 'GRG': 19, 'DRG': 19, 'VSD': 'C', 'LRG': 5},
            {'MSRP_range': '$28001 - $30000', 'MSRP_min': 28001, 'MSRP_max': 30000, 'GRG': 20, 'DRG': 20, 'VSD': 'C', 'LRG': 5},
            {'MSRP_range': '$30001 - $33000', 'MSRP_min': 30001, 'MSRP_max': 33000, 'GRG': 21, 'DRG': 21, 'VSD': 'C', 'LRG': 5},
            {'MSRP_range': '$33001 - $36000', 'MSRP_min': 33001, 'MSRP_max': 36000, 'GRG': 22, 'DRG': 22, 'VSD': 'C', 'LRG': 5},
            {'MSRP_range': '$36001 - $40000', 'MSRP_min': 36001, 'MSRP_max': 40000, 'GRG': 23, 'DRG': 23, 'VSD': 'C', 'LRG': 5},
            {'MSRP_range': '$40001 - $45000', 'MSRP_min': 40001, 'MSRP_max': 45000, 'GRG': 24, 'DRG': 24, 'VSD': 'C', 'LRG': 5},
            {'MSRP_range': '$45001 - $50000', 'MSRP_min': 45001, 'MSRP_max': 50000, 'GRG': 25, 'DRG': 25, 'VSD': 'C', 'LRG': 5},
            {'MSRP_range': '$50001 - $55000', 'MSRP_min': 50001, 'MSRP_max': 55000, 'GRG': 26, 'DRG': 26, 'VSD': 'C', 'LRG': 5},
            {'MSRP_range': '$55001 - $60000', 'MSRP_min': 55001, 'MSRP_max': 60000, 'GRG': 27, 'DRG': 27, 'VSD': 'C', 'LRG': 5},
            {'MSRP_range': '$60001 - $65000', 'MSRP_min': 60001, 'MSRP_max': 65000, 'GRG': 28, 'DRG': 28, 'VSD': 'C', 'LRG': 5},
            {'MSRP_range': '$65001 - $70000', 'MSRP_min': 65001, 'MSRP_max': 70000, 'GRG': 29, 'DRG': 29, 'VSD': 'C', 'LRG': 5},
            {'MSRP_range': '$70001 - $75000', 'MSRP_min': 70001, 'MSRP_max': 75000, 'GRG': 30, 'DRG': 30, 'VSD': 'C', 'LRG': 5}
        ]
        return pd.DataFrame(data)

    def load_fallback_vehicle_rating_groups(self) -> pd.DataFrame:
        """Loads fallback vehicle rating groups (alias for load_fallback_vehicle_ratings)."""
        return self.load_fallback_vehicle_ratings()
        
    def load_model_year_factors(self) -> pd.DataFrame:
        """Loads model year factors."""
        data = [
            {'min_year': 1999, 'bipd_factor': 1, 'coll_factor': 0.46, 'comp_factor': 0.58, 'mpc_factor': 1},
            {'min_year': 2000, 'bipd_factor': 1, 'coll_factor': 0.46, 'comp_factor': 0.58, 'mpc_factor': 1},
            {'min_year': 2001, 'bipd_factor': 1, 'coll_factor': 0.46, 'comp_factor': 0.58, 'mpc_factor': 1},
            {'min_year': 2002, 'bipd_factor': 1, 'coll_factor': 0.46, 'comp_factor': 0.58, 'mpc_factor': 1},
            {'min_year': 2003, 'bipd_factor': 1, 'coll_factor': 0.47, 'comp_factor': 0.6, 'mpc_factor': 1},
            {'min_year': 2004, 'bipd_factor': 1, 'coll_factor': 0.48, 'comp_factor': 0.62, 'mpc_factor': 1},
            {'min_year': 2005, 'bipd_factor': 1, 'coll_factor': 0.5, 'comp_factor': 0.64, 'mpc_factor': 1},
            {'min_year': 2006, 'bipd_factor': 1, 'coll_factor': 0.53, 'comp_factor': 0.67, 'mpc_factor': 1},
            {'min_year': 2007, 'bipd_factor': 1, 'coll_factor': 0.56, 'comp_factor': 0.7, 'mpc_factor': 1},
            {'min_year': 2008, 'bipd_factor': 1, 'coll_factor': 0.6, 'comp_factor': 0.72, 'mpc_factor': 1},
            {'min_year': 2009, 'bipd_factor': 1, 'coll_factor': 0.64, 'comp_factor': 0.77, 'mpc_factor': 1},
            {'min_year': 2010, 'bipd_factor': 1, 'coll_factor': 0.68, 'comp_factor': 0.79, 'mpc_factor': 1},
            {'min_year': 2011, 'bipd_factor': 1, 'coll_factor': 0.72, 'comp_factor': 0.82, 'mpc_factor': 1},
            {'min_year': 2012, 'bipd_factor': 1, 'coll_factor': 0.76, 'comp_factor': 0.84, 'mpc_factor': 1},
            {'min_year': 2013, 'bipd_factor': 1, 'coll_factor': 0.8, 'comp_factor': 0.85, 'mpc_factor': 1},
            {'min_year': 2014, 'bipd_factor': 1, 'coll_factor': 0.84, 'comp_factor': 0.91, 'mpc_factor': 1},
            {'min_year': 2015, 'bipd_factor': 1, 'coll_factor': 0.88, 'comp_factor': 0.93, 'mpc_factor': 1},
            {'min_year': 2016, 'bipd_factor': 1, 'coll_factor': 0.92, 'comp_factor': 0.95, 'mpc_factor': 1},
            {'min_year': 2017, 'bipd_factor': 1, 'coll_factor': 0.96, 'comp_factor': 0.97, 'mpc_factor': 1},
            {'min_year': 2018, 'bipd_factor': 1, 'coll_factor': 1, 'comp_factor': 1, 'mpc_factor': 1},
            {'min_year': 2019, 'bipd_factor': 1, 'coll_factor': 1.05, 'comp_factor': 1.03, 'mpc_factor': 1},
            {'min_year': 2020, 'bipd_factor': 1, 'coll_factor': 1.1, 'comp_factor': 1.06, 'mpc_factor': 1},
            {'min_year': 2021, 'bipd_factor': 1, 'coll_factor': 1.16, 'comp_factor': 1.09, 'mpc_factor': 1},
            {'min_year': 2022, 'bipd_factor': 1, 'coll_factor': 1.22, 'comp_factor': 1.12, 'mpc_factor': 1}
        ]
        df = pd.DataFrame(data)
        return df.set_index('min_year')

    def load_mileage_factors(self) -> pd.DataFrame:
        """Loads annual mileage factors."""
        data = [
            {'Annual Mileage': '0 - 1449', 'lower_bound': 0, 'BIPD': 0.617, 'COLL': 0.581, 'COMP': 0.652, 'MPC': 0.645, 'U': 0.667},
            {'Annual Mileage': '1450 - 2449', 'lower_bound': 1450, 'BIPD': 0.675, 'COLL': 0.697, 'COMP': 0.662, 'MPC': 0.674, 'U': 0.691},
            {'Annual Mileage': '2450 - 3449', 'lower_bound': 2450, 'BIPD': 0.743, 'COLL': 0.742, 'COMP': 0.7, 'MPC': 0.709, 'U': 0.733},
            {'Annual Mileage': '3450 - 4449', 'lower_bound': 3450, 'BIPD': 0.757, 'COLL': 0.78, 'COMP': 0.748, 'MPC': 0.723, 'U': 0.775},
            {'Annual Mileage': '4450 - 5449', 'lower_bound': 4450, 'BIPD': 0.816, 'COLL': 0.803, 'COMP': 0.767, 'MPC': 0.779, 'U': 0.795},
            {'Annual Mileage': '5450 - 6449', 'lower_bound': 5450, 'BIPD': 0.859, 'COLL': 0.852, 'COMP': 0.815, 'MPC': 0.783, 'U': 0.815},
            {'Annual Mileage': '6450 - 7449', 'lower_bound': 6450, 'BIPD': 0.898, 'COLL': 0.91, 'COMP': 0.853, 'MPC': 0.83, 'U': 0.892},
            {'Annual Mileage': '7450 - 8449', 'lower_bound': 7450, 'BIPD': 0.941, 'COLL': 0.951, 'COMP': 0.89, 'MPC': 0.913, 'U': 0.92},
            {'Annual Mileage': '8450 - 9449', 'lower_bound': 8450, 'BIPD': 0.957, 'COLL': 0.969, 'COMP': 0.92, 'MPC': 0.982, 'U': 0.989},
            {'Annual Mileage': '9450 - 10449', 'lower_bound': 9450, 'BIPD': 0.976, 'COLL': 0.996, 'COMP': 0.94, 'MPC': 0.989, 'U': 0.996},
            {'Annual Mileage': '10450 - 11449', 'lower_bound': 10450, 'BIPD': 1.0, 'COLL': 1.0, 'COMP': 1.0, 'MPC': 1.0, 'U': 1.0},
            {'Annual Mileage': '11450 - 12449', 'lower_bound': 11450, 'BIPD': 1.002, 'COLL': 1.002, 'COMP': 1.007, 'MPC': 1.014, 'U': 1.011},
            {'Annual Mileage': '12450 - 13449', 'lower_bound': 12450, 'BIPD': 1.006, 'COLL': 1.004, 'COMP': 1.034, 'MPC': 1.021, 'U': 1.026},
            {'Annual Mileage': '13450 - 14449', 'lower_bound': 13450, 'BIPD': 1.009, 'COLL': 1.01, 'COMP': 1.045, 'MPC': 1.028, 'U': 1.049},
            {'Annual Mileage': '14450 - 15449', 'lower_bound': 14450, 'BIPD': 1.028, 'COLL': 1.021, 'COMP': 1.069, 'MPC': 1.035, 'U': 1.062},
            {'Annual Mileage': '15450 - 16449', 'lower_bound': 15450, 'BIPD': 1.047, 'COLL': 1.029, 'COMP': 1.074, 'MPC': 1.043, 'U': 1.07},
            {'Annual Mileage': '16450 - 17449', 'lower_bound': 16450, 'BIPD': 1.069, 'COLL': 1.054, 'COMP': 1.091, 'MPC': 1.05, 'U': 1.077},
            {'Annual Mileage': '17450 - 18449', 'lower_bound': 17450, 'BIPD': 1.112, 'COLL': 1.058, 'COMP': 1.099, 'MPC': 1.057, 'U': 1.087},
            {'Annual Mileage': '18450 - 19449', 'lower_bound': 18450, 'BIPD': 1.122, 'COLL': 1.077, 'COMP': 1.155, 'MPC': 1.071, 'U': 1.096},
            {'Annual Mileage': '19450 - 20449', 'lower_bound': 19450, 'BIPD': 1.155, 'COLL': 1.104, 'COMP': 1.199, 'MPC': 1.092, 'U': 1.099},
            {'Annual Mileage': '20450 - 21449', 'lower_bound': 20450, 'BIPD': 1.168, 'COLL': 1.114, 'COMP': 1.208, 'MPC': 1.106, 'U': 1.103},
            {'Annual Mileage': '21450 - 23949', 'lower_bound': 21450, 'BIPD': 1.178, 'COLL': 1.124, 'COMP': 1.218, 'MPC': 1.121, 'U': 1.151},
            {'Annual Mileage': '23950 - 26449', 'lower_bound': 23950, 'BIPD': 1.187, 'COLL': 1.16, 'COMP': 1.285, 'MPC': 1.135, 'U': 1.166},
            {'Annual Mileage': '26450 - 28949', 'lower_bound': 26450, 'BIPD': 1.194, 'COLL': 1.206, 'COMP': 1.323, 'MPC': 1.149, 'U': 1.18},
            {'Annual Mileage': '28950 - 31449', 'lower_bound': 28950, 'BIPD': 1.215, 'COLL': 1.234, 'COMP': 1.333, 'MPC': 1.156, 'U': 1.195},
            {'Annual Mileage': '31450 - 33949', 'lower_bound': 31450, 'BIPD': 1.263, 'COLL': 1.271, 'COMP': 1.342, 'MPC': 1.16, 'U': 1.21},
            {'Annual Mileage': '33950+', 'lower_bound': 33950, 'BIPD': 1.355, 'COLL': 1.322, 'COMP': 1.362, 'MPC': 1.184, 'U': 1.224}
        ]
        df = pd.DataFrame(data)
        return df

    def load_usage_type_factors(self) -> pd.DataFrame:
        """Loads usage type factors."""
        data = [
            {'automobile_use': 'Farm', 'usage_type_code': 3, 'bipd_factor': 0.865, 'coll_factor': 0.848, 'comp_factor': 0.86, 'mpc_factor': 0.907, 'um_factor': 1.056},
            {'automobile_use': 'Pleasure / Work / School', 'usage_type_code': 1, 'bipd_factor': 1.0, 'coll_factor': 1.0, 'comp_factor': 1.0, 'mpc_factor': 1.0, 'um_factor': 1.0},
            {'automobile_use': 'Business', 'usage_type_code': 2, 'bipd_factor': 1.257, 'coll_factor': 0.918, 'comp_factor': 0.936, 'mpc_factor': 0.917, 'um_factor': 0.975}
        ]
        df = pd.DataFrame(data)
        return df.set_index('automobile_use')

    def load_single_auto_factors(self) -> pd.DataFrame:
        """Loads single automobile factors."""
        data = [
            {'coverage': 'BIPD', 'single_automobile_factor': 1.259},
            {'coverage': 'COLL', 'single_automobile_factor': 1.255},
            {'coverage': 'COMP', 'single_automobile_factor': 1.081},
            {'coverage': 'MPC', 'single_automobile_factor': 1.263},
            {'coverage': 'U', 'single_automobile_factor': 1.319}
        ]
        return pd.DataFrame(data)

    def load_annual_mileage_factors(self) -> pd.DataFrame:
        """Loads annual mileage factors (alias)."""
        return self.load_mileage_factors()

    def load_base_driver_factors(self) -> pd.DataFrame:
        """Loads State Farm specific base driver factors."""
        df = self.load_table('driver_factors/base_driver_factors  - Sheet1.csv')
        

        # Clean up whitespace in string columns used for matching
        for col in ['Marital Status', 'Years Driving']:
            if col in df.columns:
                df[col] = df[col].str.strip()
        return df

    def load_safety_driver_record_score(self) -> pd.DataFrame:
        """Loads safety driver record scoring table."""
        df = self.load_table('driver_factors/safety_driver_record_score - Sheet1.csv')
        return df

    def load_driving_safety_record_rating_plan(self) -> pd.DataFrame:
        """Loads driving safety record rating plan factors."""
        df = self.load_table('driver_factors/driving_safety_record_rating_plan - Sheet1.csv')
        return df.set_index('rate_level')

    def load_percentage_use_by_driver(self) -> pd.DataFrame:
        """Loads percentage use by driver factors."""
        df = self.load_table('driver_factors/percentage_use_by_driver - Sheet1.csv')
        return df.set_index('Key')

    def load_years_licensed_key(self) -> pd.DataFrame:
        """Loads years licensed key factors."""
        df = self.load_table('driver_factors/years_liscensed_key - Sheet1.csv')
        return df

    def load_safety_rating_factors(self) -> pd.DataFrame:
        """Loads State Farm specific safety rating factors."""
        return self.load_table('driver_factors/driving_safety_record_rating_plan - Sheet1.csv')

    def load_bi_limits(self) -> pd.DataFrame:
        """Loads bodily injury limits and factors."""
        data = [
            {'bi_limits': '30/60', 'factor': 1.03},
            {'bi_limits': '50/100', 'factor': 1.09},
            {'bi_limits': '50/150', 'factor': 1.11},
            {'bi_limits': '100/100', 'factor': 1.14},
            {'bi_limits': '100/200', 'factor': 1.17},
            {'bi_limits': '100/300', 'factor': 1.19},
            {'bi_limits': '100/500', 'factor': 1.23},
            {'bi_limits': '150/300', 'factor': 1.23},
            {'bi_limits': '200/300', 'factor': 1.27},
            {'bi_limits': '200/500', 'factor': 1.31},
            {'bi_limits': '250/500', 'factor': 1.35},
            {'bi_limits': '300/300', 'factor': 1.34},
            {'bi_limits': '300/500', 'factor': 1.38},
            {'bi_limits': '300/750', 'factor': 1.42},
            {'bi_limits': '300/1000', 'factor': 1.45},
            {'bi_limits': '500/500', 'factor': 1.47},
            {'bi_limits': '500/750', 'factor': 1.5},
            {'bi_limits': '500/1000', 'factor': 1.52},
            {'bi_limits': '750/750', 'factor': 1.55},
            {'bi_limits': '750/1000', 'factor': 1.57},
            {'bi_limits': '1000/1000', 'factor': 1.61}
        ]
        return pd.DataFrame(data)

    def load_pd_limits(self) -> pd.DataFrame:
        """Loads property damage limits and factors."""
        data = [
            {'limit': 15, 'factor': -0.03},
            {'limit': 20, 'factor': -0.01},
            {'limit': 25, 'factor': 0.0},
            {'limit': 50, 'factor': 0.05},
            {'limit': 100, 'factor': 0.07},
            {'limit': 150, 'factor': 0.08},
            {'limit': 200, 'factor': 0.1},
            {'limit': 250, 'factor': 0.11},
            {'limit': 300, 'factor': 0.13},
            {'limit': 500, 'factor': 0.16},
            {'limit': 750, 'factor': 0.2},
            {'limit': 1000, 'factor': 0.22}
        ]
        return pd.DataFrame(data)

    def load_um_limits(self) -> pd.DataFrame:
        """Loads uninsured motorist limits and factors."""
        data = [
            {'limits': '30/60', 'factor': 1.31},
            {'limits': '50/100', 'factor': 1.65},
            {'limits': '100/200', 'factor': 2.2},
            {'limits': '100/300', 'factor': 2.36},
            {'limits': '250/500', 'factor': 3.68},
            {'limits': '500/1000', 'factor': 4.62},
            {'limits': '1000/1000', 'factor': 5.0}
        ]
        return pd.DataFrame(data)

    def load_mpc_limits(self) -> pd.DataFrame:
        """Loads medical payments coverage limits and factors."""
        data = [
            {'limit': 500, 'factor': 0.34},
            {'limit': 1000, 'factor': 0.53},
            {'limit': 2000, 'factor': 0.76},
            {'limit': 3000, 'factor': 0.86},
            {'limit': 5000, 'factor': 1.0},
            {'limit': 10000, 'factor': 1.4},
            {'limit': 25000, 'factor': 2.1},
            {'limit': 50000, 'factor': 2.94},
            {'limit': 100000, 'factor': 3.78}
        ]
        return pd.DataFrame(data)

    def load_drg_deductible_factors(self) -> pd.DataFrame:
        """Loads collision deductible factors by DRG."""
        data = [
            {'DRG': 1, 'FULL': 0.2, '50': 0.16, '100': 0.13, '200': 0.11, '250': 0.09, '500': 0.07, '1000': 0.05, '1000W/20%': 0.05, '2000': 0.03},
            {'DRG': 2, 'FULL': 0.23, '50': 0.19, '100': 0.16, '200': 0.12, '250': 0.12, '500': 0.09, '1000': 0.06, '1000W/20%': 0.06, '2000': 0.03},
            {'DRG': 3, 'FULL': 0.26, '50': 0.22, '100': 0.18, '200': 0.14, '250': 0.14, '500': 0.11, '1000': 0.07, '1000W/20%': 0.06, '2000': 0.04},
            {'DRG': 4, 'FULL': 0.29, '50': 0.25, '100': 0.21, '200': 0.16, '250': 0.16, '500': 0.12, '1000': 0.08, '1000W/20%': 0.07, '2000': 0.05},
            {'DRG': 5, 'FULL': 0.35, '50': 0.3, '100': 0.27, '200': 0.18, '250': 0.18, '500': 0.14, '1000': 0.09, '1000W/20%': 0.09, '2000': 0.05},
            {'DRG': 6, 'FULL': 0.41, '50': 0.36, '100': 0.31, '200': 0.23, '250': 0.23, '500': 0.17, '1000': 0.12, '1000W/20%': 0.11, '2000': 0.07},
            {'DRG': 7, 'FULL': 0.47, '50': 0.42, '100': 0.37, '200': 0.27, '250': 0.27, '500': 0.18, '1000': 0.14, '1000W/20%': 0.14, '2000': 0.08},
            {'DRG': 8, 'FULL': 0.53, '50': 0.47, '100': 0.42, '200': 0.31, '250': 0.31, '500': 0.22, '1000': 0.16, '1000W/20%': 0.15, '2000': 0.09},
            {'DRG': 9, 'FULL': 0.59, '50': 0.53, '100': 0.48, '200': 0.36, '250': 0.35, '500': 0.27, '1000': 0.18, '1000W/20%': 0.17, '2000': 0.1},
            {'DRG': 10, 'FULL': 0.63, '50': 0.58, '100': 0.51, '200': 0.39, '250': 0.38, '500': 0.3, '1000': 0.2, '1000W/20%': 0.19, '2000': 0.11},
            {'DRG': 11, 'FULL': 0.69, '50': 0.62, '100': 0.56, '200': 0.42, '250': 0.41, '500': 0.34, '1000': 0.23, '1000W/20%': 0.22, '2000': 0.13},
            {'DRG': 12, 'FULL': 0.75, '50': 0.69, '100': 0.61, '200': 0.47, '250': 0.46, '500': 0.38, '1000': 0.27, '1000W/20%': 0.25, '2000': 0.16},
            {'DRG': 13, 'FULL': 0.85, '50': 0.79, '100': 0.72, '200': 0.56, '250': 0.55, '500': 0.45, '1000': 0.32, '1000W/20%': 0.3, '2000': 0.19},
            {'DRG': 14, 'FULL': 0.93, '50': 0.86, '100': 0.81, '200': 0.63, '250': 0.62, '500': 0.5, '1000': 0.37, '1000W/20%': 0.35, '2000': 0.23},
            {'DRG': 15, 'FULL': 1.0, '50': 0.93, '100': 0.88, '200': 0.71, '250': 0.7, '500': 0.56, '1000': 0.41, '1000W/20%': 0.38, '2000': 0.26},
            {'DRG': 16, 'FULL': 1.07, '50': 1.0, '100': 0.94, '200': 0.8, '250': 0.78, '500': 0.61, '1000': 0.45, '1000W/20%': 0.43, '2000': 0.29},
            {'DRG': 17, 'FULL': 1.15, '50': 1.07, '100': 1.02, '200': 0.89, '250': 0.84, '500': 0.67, '1000': 0.5, '1000W/20%': 0.47, '2000': 0.32},
            {'DRG': 18, 'FULL': 1.23, '50': 1.15, '100': 1.09, '200': 0.97, '250': 0.91, '500': 0.72, '1000': 0.54, '1000W/20%': 0.51, '2000': 0.35},
            {'DRG': 19, 'FULL': 1.3, '50': 1.23, '100': 1.16, '200': 1.03, '250': 0.97, '500': 0.77, '1000': 0.58, '1000W/20%': 0.54, '2000': 0.38},
            {'DRG': 20, 'FULL': 1.44, '50': 1.35, '100': 1.29, '200': 1.16, '250': 1.09, '500': 0.87, '1000': 0.66, '1000W/20%': 0.62, '2000': 0.44},
            {'DRG': 21, 'FULL': 1.59, '50': 1.5, '100': 1.43, '200': 1.29, '250': 1.21, '500': 0.98, '1000': 0.75, '1000W/20%': 0.71, '2000': 0.49},
            {'DRG': 22, 'FULL': 1.73, '50': 1.63, '100': 1.57, '200': 1.4, '250': 1.33, '500': 1.08, '1000': 0.83, '1000W/20%': 0.78, '2000': 0.53},
            {'DRG': 23, 'FULL': 1.89, '50': 1.79, '100': 1.72, '200': 1.54, '250': 1.46, '500': 1.2, '1000': 0.92, '1000W/20%': 0.87, '2000': 0.59},
            {'DRG': 24, 'FULL': 2.07, '50': 1.95, '100': 1.88, '200': 1.7, '250': 1.61, '500': 1.32, '1000': 1.03, '1000W/20%': 0.98, '2000': 0.66},
            {'DRG': 25, 'FULL': 2.25, '50': 2.13, '100': 2.05, '200': 1.86, '250': 1.76, '500': 1.45, '1000': 1.13, '1000W/20%': 1.07, '2000': 0.71},
            {'DRG': 26, 'FULL': 2.42, '50': 2.3, '100': 2.21, '200': 2.0, '250': 1.91, '500': 1.57, '1000': 1.23, '1000W/20%': 1.16, '2000': 0.77},
            {'DRG': 27, 'FULL': 2.59, '50': 2.46, '100': 2.37, '200': 2.15, '250': 2.05, '500': 1.7, '1000': 1.33, '1000W/20%': 1.24, '2000': 0.82},
            {'DRG': 28, 'FULL': 2.76, '50': 2.62, '100': 2.53, '200': 2.29, '250': 2.19, '500': 1.81, '1000': 1.43, '1000W/20%': 1.33, '2000': 0.88},
            {'DRG': 29, 'FULL': 2.93, '50': 2.78, '100': 2.68, '200': 2.45, '250': 2.33, '500': 1.94, '1000': 1.53, '1000W/20%': 1.43, '2000': 0.93},
            {'DRG': 30, 'FULL': 3.1, '50': 2.95, '100': 2.84, '200': 2.59, '250': 2.48, '500': 2.06, '1000': 1.62, '1000W/20%': 1.51, '2000': 0.97},
            {'DRG': 31, 'FULL': 3.3, '50': 3.13, '100': 3.04, '200': 2.73, '250': 2.64, '500': 2.2, '1000': 1.71, '1000W/20%': 1.59, '2000': 1.03},
            {'DRG': 32, 'FULL': 3.43, '50': 3.26, '100': 3.16, '200': 2.88, '250': 2.75, '500': 2.3, '1000': 1.8, '1000W/20%': 1.68, '2000': 1.08},
            {'DRG': 33, 'FULL': 3.6, '50': 3.42, '100': 3.32, '200': 3.02, '250': 2.89, '500': 2.42, '1000': 1.89, '1000W/20%': 1.76, '2000': 1.14},
            {'DRG': 34, 'FULL': 3.76, '50': 3.58, '100': 3.47, '200': 3.15, '250': 3.02, '500': 2.53, '1000': 1.98, '1000W/20%': 1.85, '2000': 1.19},
            {'DRG': 35, 'FULL': 3.93, '50': 3.75, '100': 3.63, '200': 3.3, '250': 3.17, '500': 2.65, '1000': 2.06, '1000W/20%': 1.92, '2000': 1.25}
        ]
        df = pd.DataFrame(data)
        return df.set_index('DRG')

    def load_grg_deductible_factors(self) -> pd.DataFrame:
        """Loads comprehensive deductible factors by GRG."""
        data = [
            {'GRG': 1, '50': 0.37, '100': 0.36, '200': 0.33, '250': 0.32, '500': 0.26, '1000': 0.15, '1000 W/20%': 0.15, '2000': 0.11},
            {'GRG': 2, '50': 0.42, '100': 0.4, '200': 0.38, '250': 0.36, '500': 0.3, '1000': 0.18, '1000 W/20%': 0.16, '2000': 0.14},
            {'GRG': 3, '50': 0.47, '100': 0.46, '200': 0.42, '250': 0.41, '500': 0.35, '1000': 0.2, '1000 W/20%': 0.19, '2000': 0.16},
            {'GRG': 4, '50': 0.54, '100': 0.49, '200': 0.47, '250': 0.46, '500': 0.39, '1000': 0.23, '1000 W/20%': 0.22, '2000': 0.18},
            {'GRG': 5, '50': 0.59, '100': 0.55, '200': 0.52, '250': 0.5, '500': 0.44, '1000': 0.26, '1000 W/20%': 0.24, '2000': 0.19},
            {'GRG': 6, '50': 0.67, '100': 0.62, '200': 0.59, '250': 0.57, '500': 0.5, '1000': 0.3, '1000 W/20%': 0.29, '2000': 0.23},
            {'GRG': 7, '50': 0.76, '100': 0.7, '200': 0.63, '250': 0.6, '500': 0.53, '1000': 0.37, '1000 W/20%': 0.35, '2000': 0.27},
            {'GRG': 8, '50': 0.81, '100': 0.75, '200': 0.69, '250': 0.65, '500': 0.56, '1000': 0.4, '1000 W/20%': 0.38, '2000': 0.3},
            {'GRG': 9, '50': 0.87, '100': 0.81, '200': 0.74, '250': 0.71, '500': 0.6, '1000': 0.44, '1000 W/20%': 0.42, '2000': 0.33},
            {'GRG': 10, '50': 0.92, '100': 0.86, '200': 0.79, '250': 0.75, '500': 0.65, '1000': 0.48, '1000 W/20%': 0.45, '2000': 0.36},
            {'GRG': 11, '50': 0.99, '100': 0.91, '200': 0.84, '250': 0.81, '500': 0.7, '1000': 0.51, '1000 W/20%': 0.48, '2000': 0.38},
            {'GRG': 12, '50': 1.07, '100': 0.99, '200': 0.91, '250': 0.87, '500': 0.77, '1000': 0.57, '1000 W/20%': 0.54, '2000': 0.44},
            {'GRG': 13, '50': 1.15, '100': 1.07, '200': 0.99, '250': 0.95, '500': 0.86, '1000': 0.62, '1000 W/20%': 0.59, '2000': 0.51},
            {'GRG': 14, '50': 1.24, '100': 1.15, '200': 1.07, '250': 1.01, '500': 0.93, '1000': 0.69, '1000 W/20%': 0.65, '2000': 0.56},
            {'GRG': 15, '50': 1.29, '100': 1.2, '200': 1.12, '250': 1.09, '500': 1.0, '1000': 0.72, '1000 W/20%': 0.68, '2000': 0.61},
            {'GRG': 16, '50': 1.35, '100': 1.26, '200': 1.18, '250': 1.16, '500': 1.06, '1000': 0.83, '1000 W/20%': 0.79, '2000': 0.65},
            {'GRG': 17, '50': 1.4, '100': 1.31, '200': 1.23, '250': 1.21, '500': 1.12, '1000': 0.88, '1000 W/20%': 0.83, '2000': 0.7},
            {'GRG': 18, '50': 1.46, '100': 1.36, '200': 1.3, '250': 1.28, '500': 1.18, '1000': 0.93, '1000 W/20%': 0.86, '2000': 0.74},
            {'GRG': 19, '50': 1.51, '100': 1.42, '200': 1.37, '250': 1.34, '500': 1.24, '1000': 0.97, '1000 W/20%': 0.92, '2000': 0.79},
            {'GRG': 20, '50': 1.59, '100': 1.5, '200': 1.42, '250': 1.41, '500': 1.3, '1000': 1.02, '1000 W/20%': 0.97, '2000': 0.84},
            {'GRG': 21, '50': 1.71, '100': 1.61, '200': 1.51, '250': 1.48, '500': 1.38, '1000': 1.1, '1000 W/20%': 1.03, '2000': 0.9},
            {'GRG': 22, '50': 1.84, '100': 1.73, '200': 1.63, '250': 1.58, '500': 1.47, '1000': 1.17, '1000 W/20%': 1.1, '2000': 0.96},
            {'GRG': 23, '50': 1.98, '100': 1.87, '200': 1.76, '250': 1.7, '500': 1.59, '1000': 1.27, '1000 W/20%': 1.19, '2000': 1.06},
            {'GRG': 24, '50': 2.13, '100': 2.0, '200': 1.89, '250': 1.85, '500': 1.73, '1000': 1.4, '1000 W/20%': 1.32, '2000': 1.16},
            {'GRG': 25, '50': 2.26, '100': 2.14, '200': 2.02, '250': 1.96, '500': 1.84, '1000': 1.55, '1000 W/20%': 1.46, '2000': 1.24},
            {'GRG': 26, '50': 2.41, '100': 2.26, '200': 2.15, '250': 2.06, '500': 1.93, '1000': 1.63, '1000 W/20%': 1.53, '2000': 1.31},
            {'GRG': 27, '50': 2.54, '100': 2.39, '200': 2.26, '250': 2.16, '500': 2.02, '1000': 1.72, '1000 W/20%': 1.62, '2000': 1.38},
            {'GRG': 28, '50': 2.69, '100': 2.52, '200': 2.39, '250': 2.27, '500': 2.12, '1000': 1.82, '1000 W/20%': 1.71, '2000': 1.46},
            {'GRG': 29, '50': 2.82, '100': 2.64, '200': 2.52, '250': 2.39, '500': 2.21, '1000': 1.91, '1000 W/20%': 1.79, '2000': 1.52},
            {'GRG': 30, '50': 2.96, '100': 2.78, '200': 2.63, '250': 2.51, '500': 2.3, '1000': 2.0, '1000 W/20%': 1.88, '2000': 1.59},
            {'GRG': 31, '50': 3.1, '100': 2.9, '200': 2.76, '250': 2.62, '500': 2.39, '1000': 2.08, '1000 W/20%': 1.95, '2000': 1.66},
            {'GRG': 32, '50': 3.24, '100': 3.03, '200': 2.88, '250': 2.74, '500': 2.48, '1000': 2.17, '1000 W/20%': 2.03, '2000': 1.73},
            {'GRG': 33, '50': 3.38, '100': 3.15, '200': 3.0, '250': 2.86, '500': 2.58, '1000': 2.26, '1000 W/20%': 2.12, '2000': 1.81},
            {'GRG': 34, '50': 3.51, '100': 3.28, '200': 3.13, '250': 2.97, '500': 2.67, '1000': 2.34, '1000 W/20%': 2.2, '2000': 1.87},
            {'GRG': 35, '50': 3.65, '100': 3.42, '200': 3.24, '250': 3.09, '500': 2.76, '1000': 2.42, '1000 W/20%': 2.26, '2000': 1.94}
        ]
        df = pd.DataFrame(data)
        return df.set_index('GRG')

    def load_good_driver_discount(self) -> pd.DataFrame:
        """Loads good driver discount factors."""
        data = [
            {'eligible': 'yes', 'factor': 0.2},
            {'eligible': 'no', 'factor': 0.0}
        ]
        df = pd.DataFrame(data)
        return df.set_index('eligible')

    def load_good_student_discount(self) -> pd.DataFrame:
        """Loads good student discount factors."""
        data = [
            {'eligible': 'yes', 'discount': '20%'},
            {'eligible': 'no', 'discount': '0'}
        ]
        df = pd.DataFrame(data)
        return df.set_index('eligible')

    def load_inexperienced_driver_education_discount(self) -> pd.DataFrame:
        """Loads inexperienced driver safety education discount factors."""
        data = [
            {'coverage': 'BIPD', 'discount_factor': 0.981},
            {'coverage': 'COLL', 'discount_factor': 0.979},
            {'coverage': 'COMP', 'discount_factor': 0.967},
            {'coverage': 'MPC', 'discount_factor': 0.819},
            {'coverage': 'UM', 'discount_factor': 0.899}
        ]
        df = pd.DataFrame(data)
        return df.set_index('coverage')

    def load_mature_driver_course_discount(self) -> pd.DataFrame:
        """Loads mature driver improvement course discount factors."""
        data = [
            {'eligible': 'yes', 'factor': 0.02},
            {'eligible': 'no', 'factor': 0.0}
        ]
        df = pd.DataFrame(data)
        return df.set_index('eligible')

    def load_multi_line_discount(self) -> pd.DataFrame:
        """Loads multiple line discount factors."""
        data = [
            {'additional_policies': "Life or Health Insurance", 'discount': 0.04, 'lookup_value': '1'},
            {'additional_policies': "Manufactured Home or Renter's Policy", 'discount': 0.09, 'lookup_value': '2'},
            {'additional_policies': "Manufactured Home/Renter's Policy + Personal Umbrella", 'discount': 0.16, 'lookup_value': '3'},
            {'additional_policies': "Condo, Homeowners, or Farm/Ranch Policy", 'discount': 0.21, 'lookup_value': '4'},
            {'additional_policies': "Condo/Homeowners/Farm-Ranch Policy + Personal Umbrella", 'discount': 0.28, 'lookup_value': '5'}
        ]
        df = pd.DataFrame(data)
        return df.set_index('lookup_value')

    def load_loyalty_discount_factors(self) -> pd.DataFrame:
        """Loads loyalty discount factors by year and coverage."""
        data = [
            {'tenure_years': 'Less than 3 years', 'bipd_factor': 0.0, 'coll_factor': 0.0, 'comp_factor': 0.0, 'mpc_factor': 0.0, 'um_factor': 0.0},
            {'tenure_years': '3 Years', 'bipd_factor': 0.11, 'coll_factor': 0.1, 'comp_factor': 0.08, 'mpc_factor': 0.04, 'um_factor': 0.09},
            {'tenure_years': '4 Years', 'bipd_factor': 0.15, 'coll_factor': 0.12, 'comp_factor': 0.12, 'mpc_factor': 0.12, 'um_factor': 0.12},
            {'tenure_years': '5 Years', 'bipd_factor': 0.17, 'coll_factor': 0.17, 'comp_factor': 0.21, 'mpc_factor': 0.17, 'um_factor': 0.15},
            {'tenure_years': '6 Years', 'bipd_factor': 0.24, 'coll_factor': 0.25, 'comp_factor': 0.29, 'mpc_factor': 0.33, 'um_factor': 0.29}
        ]
        return pd.DataFrame(data)

    def load_student_away_discount(self) -> pd.DataFrame:
        """Loads student away at school discount factors."""
        data = [
            {'coverage': 'BIPD', 'discount_factor': 0.981},
            {'coverage': 'COLL', 'discount_factor': 0.944},
            {'coverage': 'COMP', 'discount_factor': 0.974},
            {'coverage': 'MPC', 'discount_factor': 0.944},
            {'coverage': 'UM', 'discount_factor': 0.901}
        ]
        df = pd.DataFrame(data)
        return df.set_index('coverage')

    def load_loyalty_discount(self) -> pd.DataFrame:
        """Loads loyalty discount by years of coverage."""
        df = self.load_loyalty_discount_factors()
        return df.set_index('tenure_years')

    def load_car_safety_rating_discount(self) -> pd.DataFrame:
        """Loads car safety rating discount factors."""
        data = [
            {'Safety Code': 'A', 'discount': 0.39},
            {'Safety Code': 'B', 'discount': 0.3},
            {'Safety Code': 'C', 'discount': 0.16},
            {'Safety Code': 'D', 'discount': 0.08},
            {'Safety Code': 'E', 'discount': 0.0}
        ]
        df = pd.DataFrame(data)
        return df.set_index('Safety Code')

    def load_lrg_code_factors(self) -> pd.DataFrame:
        """Loads LRG code factors."""
        data = [
            {'lrg': 1, 'factor': 0.75},
            {'lrg': 2, 'factor': 0.76},
            {'lrg': 3, 'factor': 0.824},
            {'lrg': 4, 'factor': 0.95},
            {'lrg': 5, 'factor': 1.0},
            {'lrg': 6, 'factor': 1.054},
            {'lrg': 7, 'factor': 1.13},
            {'lrg': 8, 'factor': 1.16},
            {'lrg': 9, 'factor': 1.18}
        ]
        df = pd.DataFrame(data)
        return df.set_index('lrg')

    def load_transportation_network_factors(self) -> pd.DataFrame:
        """Loads transportation network company factors."""
        data = [
            {'Coverage': 'BIPD', 'Factor': 1.15},
            {'Coverage': 'COLL', 'Factor': 1.25},
            {'Coverage': 'COMP', 'Factor': 1.25},
            {'Coverage': 'MPC', 'Factor': 1.1},
            {'Coverage': 'U', 'Factor': 1.0}
        ]
        return pd.DataFrame(data)

    def load_transportation_friends_factors(self) -> pd.DataFrame:
        """Loads transportation of friends/occupation factors."""
        data = [
            {'eligbile': 'Yes', 'factor': 1.2},
            {'eligbile': 'No', 'factor': 1.0}
        ]
        return pd.DataFrame(data)

    def load_federal_employee_factors(self) -> pd.DataFrame:
        """Loads federal employee discount factors."""
        data = [
            {'eligible': 'Yes', 'factor': 0.7},
            {'eligible': 'No', 'factor': 1.0}
        ]
        return pd.DataFrame(data)

    def _create_vehicle_key(self, year, make, model, series='', package_='', style='', engine='') -> str:
        """Creates a standardized vehicle key for lookups."""
        # Ensure all parts are strings before concatenating
        parts = [str(p) for p in [year, make, model, series, package_, style, engine] if pd.notna(p)]
        return "".join(parts).upper().replace(' ', '')

# Example usage for testing:
if __name__ == '__main__':
    loader = DataLoader()
    try:
        base_rates = loader.load_base_rates()
        print("✅ Base Rates Loaded:", base_rates)
        
        territory_factors = loader.load_territory_factors()
        print("✅ Territory Factors Loaded. Shape:", territory_factors.shape)
        
        vehicle_ratings = loader.load_vehicle_ratings()
        print("✅ Vehicle Ratings Loaded. Shape:", vehicle_ratings.shape)
        
    except Exception as e:
        print(f"❌ An error occurred during data loading: {e}")
