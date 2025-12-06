import pandas as pd
import logging
from typing import List
from app.utils.data_loader import DataLoader

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
            self.vehicle_data = self.data_loader.load_table('car_factors/State Farm Auto Ratings Cleaned 2024_2001 - Sheet1_cleaned.csv')
            logger.info("Vehicle data loaded for lookup service")
    
    def get_years(self) -> List[int]:
        """Get all available years."""
        self._load_vehicle_data()
        return sorted(self.vehicle_data['YEAR'].unique().tolist())
    
    def get_makes(self, year: int) -> List[str]:
        """Get all makes for a given year."""
        self._load_vehicle_data()
        makes = self.vehicle_data[self.vehicle_data['YEAR'] == year]['MAKE'].unique().tolist()
        return sorted(makes)
    
    def get_models(self, year: int, make: str) -> List[str]:
        """Get all models for a given year and make."""
        self._load_vehicle_data()
        models = self.vehicle_data[
            (self.vehicle_data['YEAR'] == year) & 
            (self.vehicle_data['MAKE'] == make)
        ]['MODEL'].unique().tolist()
        return sorted(models)
    
    def get_series(self, year: int, make: str, model: str) -> List[str]:
        """Get all series for a given year, make, and model."""
        self._load_vehicle_data()
        series_data = self.vehicle_data[
            (self.vehicle_data['YEAR'] == year) & 
            (self.vehicle_data['MAKE'] == make) &
            (self.vehicle_data['MODEL'] == model)
        ]['SERIES'].fillna('').unique().tolist()
        
        # Ensure empty string is included if there are entries with empty series
        if '' not in series_data:
            # Check if there are any entries with empty series
            empty_series_count = len(self.vehicle_data[
                (self.vehicle_data['YEAR'] == year) & 
                (self.vehicle_data['MAKE'] == make) &
                (self.vehicle_data['MODEL'] == model) &
                (self.vehicle_data['SERIES'].isna() | (self.vehicle_data['SERIES'] == ''))
            ])
            if empty_series_count > 0:
                series_data.append('')
        
        return sorted(series_data)
    
    def get_packages(self, year: int, make: str, model: str, series: str) -> List[str]:
        """Get all packages for a given year, make, model, and series."""
        self._load_vehicle_data()
        packages = self.vehicle_data[
            (self.vehicle_data['YEAR'] == year) & 
            (self.vehicle_data['MAKE'] == make) &
            (self.vehicle_data['MODEL'] == model) &
            (self.vehicle_data['SERIES'].fillna('') == series)
        ]['OPTIONPACKAGE'].fillna('').unique().tolist()
        
        # Ensure empty string is included if there are entries with empty packages
        if '' not in packages:
            empty_package_count = len(self.vehicle_data[
                (self.vehicle_data['YEAR'] == year) & 
                (self.vehicle_data['MAKE'] == make) &
                (self.vehicle_data['MODEL'] == model) &
                (self.vehicle_data['SERIES'].fillna('') == series) &
                (self.vehicle_data['OPTIONPACKAGE'].isna() | (self.vehicle_data['OPTIONPACKAGE'] == ''))
            ])
            if empty_package_count > 0:
                packages.append('')
        
        return sorted(packages)
    
    def get_styles(self, year: int, make: str, model: str, series: str, package: str) -> List[str]:
        """Get all styles for a given year, make, model, series, and package."""
        self._load_vehicle_data()
        styles = self.vehicle_data[
            (self.vehicle_data['YEAR'] == year) & 
            (self.vehicle_data['MAKE'] == make) &
            (self.vehicle_data['MODEL'] == model) &
            (self.vehicle_data['SERIES'].fillna('') == series) &
            (self.vehicle_data['OPTIONPACKAGE'].fillna('') == package)
        ]['BODYSTYLE'].fillna('').unique().tolist()
        
        # Ensure empty string is included if there are entries with empty styles
        if '' not in styles:
            empty_style_count = len(self.vehicle_data[
                (self.vehicle_data['YEAR'] == year) & 
                (self.vehicle_data['MAKE'] == make) &
                (self.vehicle_data['MODEL'] == model) &
                (self.vehicle_data['SERIES'].fillna('') == series) &
                (self.vehicle_data['OPTIONPACKAGE'].fillna('') == package) &
                (self.vehicle_data['BODYSTYLE'].isna() | (self.vehicle_data['BODYSTYLE'] == ''))
            ])
            if empty_style_count > 0:
                styles.append('')
        
        return sorted(styles)
    
    def get_engines(self, year: int, make: str, model: str, series: str, package: str, style: str) -> List[str]:
        """Get all engines for a given year, make, model, series, package, and style."""
        self._load_vehicle_data()
        engines = self.vehicle_data[
            (self.vehicle_data['YEAR'] == year) & 
            (self.vehicle_data['MAKE'] == make) &
            (self.vehicle_data['MODEL'] == model) &
            (self.vehicle_data['SERIES'].fillna('') == series) &
            (self.vehicle_data['OPTIONPACKAGE'].fillna('') == package) &
            (self.vehicle_data['BODYSTYLE'].fillna('') == style)
        ]['ENGINE'].fillna('').unique().tolist()
        
        # Ensure empty string is included if there are entries with empty engines
        if '' not in engines:
            empty_engine_count = len(self.vehicle_data[
                (self.vehicle_data['YEAR'] == year) & 
                (self.vehicle_data['MAKE'] == make) &
                (self.vehicle_data['MODEL'] == model) &
                (self.vehicle_data['SERIES'].fillna('') == series) &
                (self.vehicle_data['OPTIONPACKAGE'].fillna('') == package) &
                (self.vehicle_data['BODYSTYLE'].fillna('') == style) &
                (self.vehicle_data['ENGINE'].isna() | (self.vehicle_data['ENGINE'] == ''))
            ])
            if empty_engine_count > 0:
                engines.append('')
        
        return sorted(engines)
    
    def get_rating_groups(self, year: int, make: str, model: str, series: str, package: str, style: str, engine: str) -> dict:
        """Get rating groups for a complete vehicle specification."""
        self._load_vehicle_data()
        
        # Find the matching vehicle
        vehicle_data = self.vehicle_data[
            (self.vehicle_data['YEAR'] == year) & 
            (self.vehicle_data['MAKE'] == make) &
            (self.vehicle_data['MODEL'] == model) &
            (self.vehicle_data['SERIES'].fillna('') == series) &
            (self.vehicle_data['OPTIONPACKAGE'].fillna('') == package) &
            (self.vehicle_data['BODYSTYLE'].fillna('') == style) &
            (self.vehicle_data['ENGINE'].fillna('') == engine)
        ]
        # TODO. 
        if not vehicle_data:
             vehicle_data = fetchVehicleDataUsingRAG(year, make, model, series, package, style, engine)
        if len(vehicle_data) > 0:
            row = vehicle_data.iloc[0]
            return {
                'drg': int(row['DRG']),
                'grg': int(row['GRG']),
                'vsd': str(row['VSD']),
                'lrg': int(row['LRG'])
            }
        else:
            # Return default values if not found
            return {'drg': 1, 'grg': 1, 'vsd': '1', 'lrg': 1}
    
    def search_vehicles(self, make: str = None, model: str = None, year: int = None) -> List[dict]:
        """
        Search for vehicles based on make, model, and year criteria.
        Returns a list of matching vehicles with their details.
        """
        self._load_vehicle_data()
        
        # Start with all data
        filtered = self.vehicle_data.copy()
        
        # Apply filters
        if year is not None:
            filtered = filtered[filtered['YEAR'] == year]
        
        if make is not None:
            filtered = filtered[filtered['MAKE'].str.upper().str.contains(make.upper(), na=False)]
        
        if model is not None:
            filtered = filtered[filtered['MODEL'].str.upper().str.contains(model.upper(), na=False)]
        
        # Convert to list of dictionaries
        results = []
        for _, row in filtered.iterrows():
            vehicle = {
                'year': int(row['YEAR']),
                'make': row['MAKE'],
                'model': row['MODEL'],
                'series': row['SERIES'] if pd.notna(row['SERIES']) else '',
                'package': row['OPTIONPACKAGE'] if pd.notna(row['OPTIONPACKAGE']) else '',
                'style': row['BODYSTYLE'] if pd.notna(row['BODYSTYLE']) else '',
                'engine': row['ENGINE'] if pd.notna(row['ENGINE']) else '',
                'wheelbase': row['Wheelbase'] if pd.notna(row['Wheelbase']) else '',
                'grg': int(row['GRG']) if pd.notna(row['GRG']) else None,
                'drg': int(row['DRG']) if pd.notna(row['DRG']) else None,
                'vsd': row['VSD'] if pd.notna(row['VSD']) else None,
                'lrg': int(row['LRG']) if pd.notna(row['LRG']) else None
            }
            results.append(vehicle)
        
        logger.info(f"Found {len(results)} vehicles matching criteria: make={make}, model={model}, year={year}")
        return results
