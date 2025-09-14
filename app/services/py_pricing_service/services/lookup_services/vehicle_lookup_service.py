import pandas as pd
import logging
from typing import List
from app.services.py_pricing_service.utils.data_loader import DataLoader

logger = logging.getLogger(__name__)

class VehicleLookupService:
    """
    Service for providing vehicle data for cascading dropdowns.
    """
    
    def __init__(self):
        self.data_loader = DataLoader()
        self.vehicle_data: pd.DataFrame = None
        
    def _load_vehicle_data(self):
        """Load vehicle data if not already loaded."""
        if self.vehicle_data is None:
            self.vehicle_data = self.data_loader.load_table('car_factors/vehicle_ratings_groups - Sheet1.csv')
            logger.info("Vehicle data loaded for lookup service")
    
    def get_years(self) -> List[int]:
        """Get all available years."""
        self._load_vehicle_data()
        return sorted(self.vehicle_data['year'].unique().tolist())
    
    def get_makes(self, year: int) -> List[str]:
        """Get all makes for a given year."""
        self._load_vehicle_data()
        makes = self.vehicle_data[self.vehicle_data['year'] == year]['make'].unique().tolist()
        return sorted(makes)
    
    def get_models(self, year: int, make: str) -> List[str]:
        """Get all models for a given year and make."""
        self._load_vehicle_data()
        models = self.vehicle_data[
            (self.vehicle_data['year'] == year) & 
            (self.vehicle_data['make'] == make)
        ]['model'].unique().tolist()
        return sorted(models)
    
    def get_series(self, year: int, make: str, model: str) -> List[str]:
        """Get all series for a given year, make, and model."""
        self._load_vehicle_data()
        series_data = self.vehicle_data[
            (self.vehicle_data['year'] == year) & 
            (self.vehicle_data['make'] == make) &
            (self.vehicle_data['model'] == model)
        ]['series'].fillna('').unique().tolist()
        
        # Ensure empty string is included if there are entries with empty series
        if '' not in series_data:
            # Check if there are any entries with empty series
            empty_series_count = len(self.vehicle_data[
                (self.vehicle_data['year'] == year) & 
                (self.vehicle_data['make'] == make) &
                (self.vehicle_data['model'] == model) &
                (self.vehicle_data['series'].isna() | (self.vehicle_data['series'] == ''))
            ])
            if empty_series_count > 0:
                series_data.append('')
        
        return sorted(series_data)
    
    def get_packages(self, year: int, make: str, model: str, series: str) -> List[str]:
        """Get all packages for a given year, make, model, and series."""
        self._load_vehicle_data()
        packages = self.vehicle_data[
            (self.vehicle_data['year'] == year) & 
            (self.vehicle_data['make'] == make) &
            (self.vehicle_data['model'] == model) &
            (self.vehicle_data['series'].fillna('') == series)
        ]['package'].fillna('').unique().tolist()
        
        # Ensure empty string is included if there are entries with empty packages
        if '' not in packages:
            empty_package_count = len(self.vehicle_data[
                (self.vehicle_data['year'] == year) & 
                (self.vehicle_data['make'] == make) &
                (self.vehicle_data['model'] == model) &
                (self.vehicle_data['series'].fillna('') == series) &
                (self.vehicle_data['package'].isna() | (self.vehicle_data['package'] == ''))
            ])
            if empty_package_count > 0:
                packages.append('')
        
        return sorted(packages)
    
    def get_styles(self, year: int, make: str, model: str, series: str, package: str) -> List[str]:
        """Get all styles for a given year, make, model, series, and package."""
        self._load_vehicle_data()
        styles = self.vehicle_data[
            (self.vehicle_data['year'] == year) & 
            (self.vehicle_data['make'] == make) &
            (self.vehicle_data['model'] == model) &
            (self.vehicle_data['series'].fillna('') == series) &
            (self.vehicle_data['package'].fillna('') == package)
        ]['style'].fillna('').unique().tolist()
        
        # Ensure empty string is included if there are entries with empty styles
        if '' not in styles:
            empty_style_count = len(self.vehicle_data[
                (self.vehicle_data['year'] == year) & 
                (self.vehicle_data['make'] == make) &
                (self.vehicle_data['model'] == model) &
                (self.vehicle_data['series'].fillna('') == series) &
                (self.vehicle_data['package'].fillna('') == package) &
                (self.vehicle_data['style'].isna() | (self.vehicle_data['style'] == ''))
            ])
            if empty_style_count > 0:
                styles.append('')
        
        return sorted(styles)
    
    def get_engines(self, year: int, make: str, model: str, series: str, package: str, style: str) -> List[str]:
        """Get all engines for a given year, make, model, series, package, and style."""
        self._load_vehicle_data()
        engines = self.vehicle_data[
            (self.vehicle_data['year'] == year) & 
            (self.vehicle_data['make'] == make) &
            (self.vehicle_data['model'] == model) &
            (self.vehicle_data['series'].fillna('') == series) &
            (self.vehicle_data['package'].fillna('') == package) &
            (self.vehicle_data['style'].fillna('') == style)
        ]['engine'].fillna('').unique().tolist()
        
        # Ensure empty string is included if there are entries with empty engines
        if '' not in engines:
            empty_engine_count = len(self.vehicle_data[
                (self.vehicle_data['year'] == year) & 
                (self.vehicle_data['make'] == make) &
                (self.vehicle_data['model'] == model) &
                (self.vehicle_data['series'].fillna('') == series) &
                (self.vehicle_data['package'].fillna('') == package) &
                (self.vehicle_data['style'].fillna('') == style) &
                (self.vehicle_data['engine'].isna() | (self.vehicle_data['engine'] == ''))
            ])
            if empty_engine_count > 0:
                engines.append('')
        
        return sorted(engines)
    
    def get_rating_groups(self, year: int, make: str, model: str, series: str, package: str, style: str, engine: str) -> dict:
        """Get rating groups for a complete vehicle specification."""
        self._load_vehicle_data()
        
        # Find the matching vehicle
        vehicle_data = self.vehicle_data[
            (self.vehicle_data['year'] == year) & 
            (self.vehicle_data['make'] == make) &
            (self.vehicle_data['model'] == model) &
            (self.vehicle_data['series'].fillna('') == series) &
            (self.vehicle_data['package'].fillna('') == package) &
            (self.vehicle_data['style'].fillna('') == style) &
            (self.vehicle_data['engine'].fillna('') == engine)
        ]
        
        if len(vehicle_data) > 0:
            row = vehicle_data.iloc[0]
            return {
                'drg': int(row['drg']),
                'grg': int(row['grg']),
                'vsd': str(row['vsd']),
                'lrg': int(row['lrg'])
            }
        else:
            # Return default values if not found
            return {'drg': 1, 'grg': 1, 'vsd': '1', 'lrg': 1}
