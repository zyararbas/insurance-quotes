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
        df = self.load_table('coverage_factors/bi_limits - Sheet1.csv')
        return df

    def load_pd_limits(self) -> pd.DataFrame:
        """Loads property damage limits and factors."""
        df = self.load_table('coverage_factors/pd_limits - Sheet1.csv')
        return df

    def load_um_limits(self) -> pd.DataFrame:
        """Loads uninsured motorist limits and factors."""
        df = self.load_table('coverage_factors/um_limits - Sheet1.csv')
        return df

    def load_mpc_limits(self) -> pd.DataFrame:
        """Loads medical payments coverage limits and factors."""
        df = self.load_table('coverage_factors/mpc_coverage_limits - Sheet1.csv')
        return df

    def load_drg_deductible_factors(self) -> pd.DataFrame:
        """Loads collision deductible factors by DRG."""
        df = self.load_table('coverage_factors/drg_deductible_factors - Sheet1.csv')
        return df.set_index('DRG')

    def load_grg_deductible_factors(self) -> pd.DataFrame:
        """Loads comprehensive deductible factors by GRG."""
        df = self.load_table('coverage_factors/grg_deductible  - Sheet1.csv')
        return df.set_index('GRG')

    def load_good_driver_discount(self) -> pd.DataFrame:
        """Loads good driver discount factors."""
        df = self.load_table('discounts/good_driver_discount - Sheet1.csv')
        return df.set_index('eligible')

    def load_good_student_discount(self) -> pd.DataFrame:
        """Loads good student discount factors."""
        df = self.load_table('discounts/good_student_discount - Sheet1.csv')
        return df.set_index('eligible')

    def load_inexperienced_driver_education_discount(self) -> pd.DataFrame:
        """Loads inexperienced driver safety education discount factors."""
        df = self.load_table('discounts/inexperienced_driver_safety_education_discount - Sheet1.csv')
        return df.set_index('coverage')

    def load_mature_driver_course_discount(self) -> pd.DataFrame:
        """Loads mature driver improvement course discount factors."""
        df = self.load_table('discounts/mature_driver_improvement_course_discount - Sheet1.csv')
        return df.set_index('eligible')

    def load_multi_line_discount(self) -> pd.DataFrame:
        """Loads multiple line discount factors."""
        # Use standard CSV loading since policy names are now quoted
        df = self.load_table('discounts/mulitple_line_discount - Sheet1.csv')
        # Convert lookup_value to string for consistent indexing
        df['lookup_value'] = df['lookup_value'].astype(str)
        return df.set_index('lookup_value')

    def load_loyalty_discount_factors(self) -> pd.DataFrame:
        """Loads loyalty discount factors by year and coverage."""
        df = self.load_table('discounts/loyalty_discount_by_year_coverage - Sheet1.csv')
        return df

    def load_student_away_discount(self) -> pd.DataFrame:
        """Loads student away at school discount factors."""
        df = self.load_table('discounts/student_away_at_school_discount - Sheet1.csv')
        return df.set_index('coverage')

    def load_loyalty_discount(self) -> pd.DataFrame:
        """Loads loyalty discount by years of coverage."""
        df = self.load_table('discounts/loyalty_discount_by_year_coverage - Sheet1.csv')
        return df.set_index('tenure_years')

    def load_car_safety_rating_discount(self) -> pd.DataFrame:
        """Loads car safety rating discount factors."""
        df = self.load_table('discounts/car_safety_rating_discount - Sheet1.csv')
        return df.set_index('Safety Code')

    def load_lrg_code_factors(self) -> pd.DataFrame:
        """Loads LRG code factors."""
        df = self.load_table('coverage_factors/lrg_code_factors - Sheet1.csv')
        return df.set_index('lrg')

    def load_transportation_network_factors(self) -> pd.DataFrame:
        """Loads transportation network company factors."""
        df = self.load_table('coverage_factors/transporation_network_company - Sheet1.csv')
        return df

    def load_transportation_friends_factors(self) -> pd.DataFrame:
        """Loads transportation of friends/occupation factors."""
        df = self.load_table('coverage_factors/transportation of friends_or_occupation - Sheet1.csv')
        return df

    def load_federal_employee_factors(self) -> pd.DataFrame:
        """Loads federal employee discount factors."""
        df = self.load_table('coverage_factors/federal_employee - Sheet1.csv')
        return df

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
