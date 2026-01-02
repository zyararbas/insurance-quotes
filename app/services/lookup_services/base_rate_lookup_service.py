import pandas as pd
import logging
from typing import Dict, List
from app.utils.data_loader import DataLoader

logger = logging.getLogger(__name__)
ZIP_TERRITORY_FACTORS_COLLECTION = "zip-territory-factors"
BASE_RATES = {
    "BIPD": 841.98,
    "COLL": 869.39,
    "COMP": 50,
    "MPC": 91.88,
    "U": 86.2,
    "Car Rental (R1)": 20.52
}

class BaseRateLookupService:
    """
    Microservice for base rate lookups.
    Handles territory factors and base rates for each coverage type.
    """
    
    def __init__(self, carrier_config: dict):
        self.carrier_config = carrier_config
        self.data_loader = DataLoader()
        #self.base_rates: pd.DataFrame = None
        # self.zip_territory_factors: pd.DataFrame = None
        
    def initialize(self):
        """Loads base rate and territory factor data."""
        #self.base_rates = self.data_loader.load_base_rates()
        # self.zip_territory_factors = self.data_loader.load_zip_territory_factors()
        logger.info("BaseRateLookupService initialized")
        
    def get_territory_factor_for_coverage(self, zip_factors: Dict, coverage: str) -> float:
            # Get the specific factor for this coverage type
        factor_column = f"{coverage.lower()}_factor"
        if factor_column in zip_factors: 
            factor = zip_factors[factor_column]
            logger.info(f"Territory factor for {coverage}: {factor}")
            return float(factor)
        else:
            logger.warning(f"No territory factor column found for {coverage}")
            return 1.0
       

    def get_territory_factors_by_zip(self, zip_code: str) -> float:
        """Gets territory factor for a specific zip code."""
        # if self.zip_territory_factors is None:
        #     self.initialize()
            
        try:
            # Convert zip code to integer for lookup
            zip_int = int(zip_code)
            
            # Check if zip code exists in the index
            # if zip_int not in self.zip_territory_factors.index:
            #     logger.warning(f"Zip code {zip_code} not found in territory table")
            #     return 1.0  # Default neutral factor
            
            # Get the row for this zip code
            # row = self.zip_territory_factors.loc[zip_int]

            # O(1) query into mongodb database to retrieve entire factor as a list
            query = {"_id": zip_int}
            row =  self.data_loader.storage_service.find(query, ZIP_TERRITORY_FACTORS_COLLECTION)
            factors = row[0]
            return factors
        except Exception as e:
            logger.error(f"Error getting territory factor for zip {zip_code}: {e}")
            return 1.0

    def get_base_rate(self, coverage: str) -> float:
        """Gets base rate for a specific coverage type."""
        

        # if self.base_rates is None:
        #     self.initialize()
            
        try:
            # Handle coverage name mapping (UM -> U in base rates table)
            coverage_key = coverage
            if coverage == 'UM':
                coverage_key = 'U'
            
            # Base rates are stored as a dictionary {coverage: rate}
            if coverage_key in BASE_RATES:
                base_rate = BASE_RATES[coverage_key]
                logger.info(f"Base rate for {coverage} (mapped to {coverage_key}): {base_rate}")
                return float(base_rate)
            else:
                logger.warning(f"No base rate found for {coverage} (mapped to {coverage_key})")
                return 0.0
                
        except Exception as e:
            logger.error(f"Error getting base rate for {coverage}: {e}")
            return 0.0
            
    def calculate_base_factors(self, zip_code: str, coverages: List[str]) -> Dict:
        """
        Calculates base factors (territory-adjusted base rates) for all coverages.
        Returns: {coverage: {'territory_factor': float, 'base_rate': float, 'territorial_rate': float}}
        """
        # if self.zip_territory_factors is None or self.base_rates is None:
        #     self.initialize()
            
        results = {}
        zip_factors = self.get_territory_factors_by_zip(zip_code)
        for coverage in coverages:
            territory_factor = self.get_territory_factor_for_coverage(zip_factors, coverage)
            base_rate = self.get_base_rate(coverage)
            territorial_rate = base_rate * territory_factor
            
            results[coverage] = {
                'territory_factor': territory_factor,
                'base_rate': base_rate,
                'territorial_rate': territorial_rate
            }
            
            logger.info(f"Base factors for {coverage}: territory={territory_factor}, base={base_rate}, final={territorial_rate}")
            
        return results
