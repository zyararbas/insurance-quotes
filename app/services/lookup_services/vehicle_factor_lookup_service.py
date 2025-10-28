import pandas as pd
import logging
from typing import Dict, List, Optional
from utils.data_loader import DataLoader        
from models.models import Vehicle, Usage

logger = logging.getLogger(__name__)

class VehicleFactorLookupService:
    """
    Microservice for vehicle factor lookups.
    Handles vehicle rating groups, model year factors, and LRG factors.
    """
    
    def __init__(self, carrier_config: dict):
        self.carrier_config = carrier_config
        self.data_loader = DataLoader()
        
        # Vehicle rating tables
        self.vehicle_rating_groups: pd.DataFrame = None
        self.fallback_vehicle_rating_groups: pd.DataFrame = None
        self.model_year_factors: pd.DataFrame = None
        self.lrg_factors: pd.DataFrame = None
        
    def initialize(self):
        """Loads all vehicle factor data tables."""
        self.vehicle_rating_groups = self.data_loader.load_vehicle_ratings_groups()
        self.fallback_vehicle_rating_groups = self.data_loader.load_fallback_vehicle_rating_groups()
        self.model_year_factors = self.data_loader.load_model_year_factors()
        self.lrg_factors = self.data_loader.load_lrg_code_factors()
        
        logger.info("VehicleFactorLookupService initialized")
        
    def get_vehicle_rating_groups(self, vehicle: Vehicle) -> Dict[str, int]:
        """
        Gets the vehicle rating groups (DRG, GRG, VSD, LRG) for a specific vehicle.
        Returns: {'drg': int, 'grg': int, 'vsd': str, 'lrg': int}
        """
        if self.vehicle_rating_groups is None:
            self.initialize()
            
        try:
            # Build the lookup key using the same format as the data loader
            parts = [str(vehicle.year), vehicle.make, vehicle.model]
            if vehicle.series:
                parts.append(vehicle.series)
            if vehicle.package:
                parts.append(vehicle.package)
            if vehicle.style:
                parts.append(vehicle.style)
            if vehicle.engine:
                parts.append(vehicle.engine)
            
            lookup_key = "".join(parts).upper().replace(' ', '')
            
            # Try to find exact match
            if lookup_key in self.vehicle_rating_groups.index:
                match = self.vehicle_rating_groups.loc[lookup_key]
                rating_groups = {
                    'drg': int(match['drg']),
                    'grg': int(match['grg']),
                    'vsd': str(match['vsd']),
                    'lrg': int(match['lrg'])
                }
                logger.info(f"Vehicle rating groups for {lookup_key}: {rating_groups}")
                return rating_groups
            else:
                # Try fallback with fewer components
                fallback_key = f"{vehicle.year}_{vehicle.make}_{vehicle.model}"
                fallback_match = self.fallback_vehicle_rating_groups[
                    self.fallback_vehicle_rating_groups['vehicle_key'] == fallback_key
                ]
                
                if not fallback_match.empty:
                    rating_groups = {
                        'drg': int(fallback_match['drg'].iloc[0]),
                        'grg': int(fallback_match['grg'].iloc[0]),
                        'vsd': str(fallback_match['vsd'].iloc[0]),
                        'lrg': int(fallback_match['lrg'].iloc[0])
                    }
                    logger.info(f"Fallback vehicle rating groups for {fallback_key}: {rating_groups}")
                    return rating_groups
                    
                # If still no match, use default values
                logger.warning(f"No vehicle rating groups found for {lookup_key}, using defaults")
                return {'drg': 1, 'grg': 1, 'vsd': '1', 'lrg': 1}
                
        except Exception as e:
            logger.error(f"Error getting vehicle rating groups: {e}")
            return {'drg': 1, 'grg': 1, 'vsd': '1', 'lrg': 1}
            
    def get_model_year_factor(self, coverage: str, year: int) -> float:
        """Gets the model year factor for a specific coverage and vehicle year."""
        if self.model_year_factors is None:
            self.initialize()
            
        try:
            # Since the DataFrame is indexed by min_year, find the highest min_year that is <= year
            best_min_year = -1
            
            # Get all min_year values that are <= the target year
            valid_years = self.model_year_factors.index[self.model_year_factors.index <= year]
            
            if len(valid_years) > 0:
                # Get the highest (maximum) min_year that is <= year
                best_min_year = valid_years.max()
                
                # Get the factor for this coverage and year
                factor = self.model_year_factors.loc[best_min_year, coverage.lower() + '_factor']
                logger.info(f"Model year factor for {coverage} at year {year}: {factor}")
                return float(factor)
                    
            logger.warning(f"No model year factor found for {coverage} at year {year}, using default 1.0")
            return 1.0
            
        except Exception as e:
            logger.error(f"Error getting model year factor for {coverage} at year {year}: {e}")
            return 1.0
            
    def get_lrg_factor(self, coverage: str, lrg_code: int) -> float:
        """Gets the LRG factor for a specific coverage and LRG code."""
        if self.lrg_factors is None:
            self.initialize()
            
        try:
            # Since the DataFrame is indexed by 'lrg', we can access it directly
            if lrg_code in self.lrg_factors.index:
                factor = self.lrg_factors.loc[lrg_code, 'factor']
                logger.info(f"LRG factor for {coverage} at LRG {lrg_code}: {factor}")
                return float(factor)
            else:
                logger.warning(f"No LRG factor found for {coverage} at LRG {lrg_code}, using default 1.0")
                return 1.0
                
        except Exception as e:
            logger.error(f"Error getting LRG factor for {coverage} at LRG {lrg_code}: {e}")
            return 1.0
            
    def calculate_vehicle_factors(self, vehicle: Vehicle, usage: Usage, coverages: List[str]) -> Dict:
        """
        Calculates all vehicle factors for the given coverages.
        Returns: {coverage: {'combined_factor': float, 'breakdown': dict}}
        """
        if self.vehicle_rating_groups is None:
            self.initialize()
            
        # Get vehicle rating groups
        rating_groups = self.get_vehicle_rating_groups(vehicle)
        
        results = {}
        for coverage in coverages:
            # Model year factor
            model_year_factor = self.get_model_year_factor(coverage, vehicle.year)
            
            # LRG factor (only for collision and comprehensive)
            lrg_factor = 1.0
            if coverage in ['COLL', 'COMP']:
                lrg_factor = self.get_lrg_factor(coverage, rating_groups['lrg'])
            
            # Combined vehicle factor
            combined_factor = model_year_factor * lrg_factor
            
            results[coverage] = {
                'combined_factor': combined_factor,
                'breakdown': {
                    'model_year_factor': model_year_factor,
                    'lrg_factor': lrg_factor,
                    'rating_groups': rating_groups
                }
            }
            
            logger.info(f"Vehicle factors for {coverage}: model_year={model_year_factor}, lrg={lrg_factor}, combined={combined_factor}")
            
        return results
