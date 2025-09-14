import pandas as pd
import os
from functools import lru_cache
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataLoader:
    """
    Handles loading and caching of insurance rating tables from CSV files.
    Provides efficient access to data using pandas and in-memory caching.
    """
    def __init__(self):
        # Assumes the 'Data' directory is at the app level.
        self.base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'Data', 'California', 'STATEFARM_CA_Insurance__tables'))
        logger.info(f"DataLoader initialized with base path: {self.base_path}")
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

    def load_zip_territory_factors(self) -> pd.DataFrame:
        """Loads zip territory factors (alias for load_territory_factors)."""
        return self.load_territory_factors()

    def load_vehicle_ratings(self) -> pd.DataFrame:
        """Loads vehicle ratings groups."""
        df = self.load_table('car_factors/vehicle_ratings_groups - Sheet1.csv')
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

    def load_vehicle_ratings_groups(self) -> pd.DataFrame:
        """Loads vehicle ratings groups (alias for load_vehicle_ratings)."""
        return self.load_vehicle_ratings()

    def load_fallback_vehicle_ratings(self) -> pd.DataFrame:
        """Loads fallback vehicle ratings (by MSRP)."""
        df = self.load_table('car_factors/fallback_vehicle_rating_groups - Sheet1.csv')
        return df

    def load_fallback_vehicle_rating_groups(self) -> pd.DataFrame:
        """Loads fallback vehicle rating groups (alias for load_fallback_vehicle_ratings)."""
        return self.load_fallback_vehicle_ratings()
        
    def load_model_year_factors(self) -> pd.DataFrame:
        """Loads model year factors."""
        df = self.load_table('car_factors/model_year_factors - Sheet1.csv')
        return df.set_index('min_year')

    def load_mileage_factors(self) -> pd.DataFrame:
        """Loads annual mileage factors."""
        df = self.load_table('car_factors/annual_mileage_factor - Sheet1.csv')
        # Rename columns to be more Python-friendly
        df.rename(columns={'Lower Bound': 'lower_bound'}, inplace=True)
        return df

    def load_usage_type_factors(self) -> pd.DataFrame:
        """Loads usage type factors."""
        df = self.load_table('car_factors/auto_usage_type - Sheet1.csv')
        return df.set_index('automobile_use')

    def load_single_auto_factors(self) -> pd.DataFrame:
        """Loads single automobile factors."""
        return self.load_table('car_factors/single_auto_factor - Sheet1.csv')

    def load_annual_mileage_factors(self) -> pd.DataFrame:
        """Loads annual mileage factors."""
        df = self.load_table('car_factors/annual_mileage_factor - Sheet1.csv')
        df.rename(columns={'Lower Bound': 'lower_bound'}, inplace=True)
        return df

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
