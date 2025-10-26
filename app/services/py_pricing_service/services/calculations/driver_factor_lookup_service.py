import pandas as pd
from typing import Dict, Any
from utils.data_loader import DataLoader

class DriverFactorLookupService:
    """Service for driver factor lookups used by the frontend."""
    
    def __init__(self):
        self.data_loader = DataLoader()
        self._territory_factors: pd.DataFrame = None
        self._base_driver_factors: pd.DataFrame = None
        self._safety_rating_plan: pd.DataFrame = None
        self._percentage_use: pd.DataFrame = None
        self._years_licensed: pd.DataFrame = None
        self._single_auto_factors: pd.DataFrame = None
        self._safety_rating_factors: pd.DataFrame = None

    def get_zip_factors(self, zip_code: str) -> Dict[str, Any]:
        """Get territory factors for a specific zip code."""
        if self._territory_factors is None:
            self._territory_factors = self.data_loader.load_territory_factors()
        
        # Convert zip code to integer for lookup since the index is int64
        try:
            zip_int = int(zip_code)
        except ValueError:
            raise ValueError(f"Invalid zip code format: {zip_code}")
        
        if zip_int not in self._territory_factors.index:
            raise ValueError(f"Zip code {zip_code} not found in territory factors")
        
        row = self._territory_factors.loc[zip_int]
        
        return {
            "zip_code": zip_code,
            "factors": {
                "BIPD": float(row['bipd_factor']),
                "COLL": float(row['coll_factor']),
                "COMP": float(row['comp_factor']),
                "MPC": float(row['mpc_factor']),
                "UM": float(row['um_factor']),
                "ERS": float(row['ers_factor'])
            },
            "source_table": "CA_zip_territory_factors"
        }

    def get_driver_factors(self, marital_status: str, years_licensed: int, assigned_driver: str) -> Dict[str, Any]:
        """Get base driver factors."""
        if self._base_driver_factors is None:
            self._base_driver_factors = self.data_loader.load_base_driver_factors()
        
        # Convert years licensed to lookup key based on the CSV structure
        if assigned_driver.lower() == 'yes':
            if years_licensed <= 13:
                lookup_marital = marital_status if marital_status in ['S', 'M'] else 'S'
                lookup_years = str(years_licensed)
            else:
                lookup_marital = 'All Not\n Specifically\n Listed'
                lookup_years = str(years_licensed)  # Years driving is still numeric for 14+
        else:
            lookup_marital = 'All Not\n Specifically\n Listed'
            lookup_years = 'All Not\n Specifically\n Listed'
        
        # Find factors for each coverage type
        factors = {}
        for coverage in ['BIPD', 'COLL', 'COMP', 'MPC', 'U']:
            match = self._base_driver_factors[
                (self._base_driver_factors['Coverage'] == coverage) &
                (self._base_driver_factors['Assigned Driver'] == assigned_driver) &
                (self._base_driver_factors['Marital Status'] == lookup_marital) &
                (self._base_driver_factors['Years Driving'] == lookup_years)
            ]
            
            if not match.empty:
                factors[coverage] = float(match['Factor'].iloc[0])
            else:
                # Fallback to default
                fallback = self._base_driver_factors[
                    (self._base_driver_factors['Coverage'] == coverage) &
                    (self._base_driver_factors['Assigned Driver'] == assigned_driver) &
                    (self._base_driver_factors['Marital Status'] == 'All Not Specifically Listed') &
                    (self._base_driver_factors['Years Driving'] == 'All Not Specifically Listed')
                ]
                factors[coverage] = float(fallback['Factor'].iloc[0]) if not fallback.empty else 1.0
        
        return {
            "marital_status": marital_status,
            "years_licensed": years_licensed,
            "assigned_driver": assigned_driver,
            "lookup_marital_status": lookup_marital,
            "lookup_years_driving": lookup_years,
            "factors": factors,
            "source_table": "base_driver_factors"
        }

    def get_safety_factors_for_level(self, safety_record_level: int) -> Dict[str, Any]:
        """Get safety record factors for a given safety record level."""
        if self._safety_rating_plan is None:
            self._safety_rating_plan = self.data_loader.load_driving_safety_record_rating_plan()
        
        if safety_record_level not in self._safety_rating_plan.index:
            raise ValueError(f"Safety record level {safety_record_level} not found")
        
        row = self._safety_rating_plan.loc[safety_record_level]
        
        return {
            "safety_record_level": safety_record_level,
            "factors": {
                "BIPD": float(row['bipd_factor']),
                "COLL": float(row['coll_factor']),
                "COMP": float(row['comp_factor']),
                "MPC": float(row['mpc_factor']),
                "UM": float(row['um_factor'])
            },
            "source_table": "driving_safety_record_rating_plan"
        }

    def get_percentage_factors(self, assigned_driver: str, occasional_driver: str) -> Dict[str, Any]:
        """Get percentage usage factors."""
        if self._percentage_use is None:
            self._percentage_use = self.data_loader.load_percentage_use_by_driver()
        
        # Determine lookup key based on assignment and occasional driver status
        if assigned_driver.lower() == 'yes':
            if occasional_driver.lower() == 'yes':
                lookup_key = "All Not Specifically Listed, Yes"
            else:
                lookup_key = "Yes, No"
        else:
            lookup_key = "No, No"
        
        if lookup_key not in self._percentage_use.index:
            # Fallback to most common case
            lookup_key = "No, No"
        
        if lookup_key in self._percentage_use.index:
            row = self._percentage_use.loc[lookup_key]
            
            return {
                "assigned_driver": assigned_driver,
                "occasional_driver": occasional_driver,
                "lookup_key": lookup_key,
                "factors": {
                    "BIPD": float(row['BIPD']),
                    "COLL": float(row['COLL']),
                    "COMP": float(row['COMP']),
                    "MPC": float(row['MPC']),
                    "U": float(row['U'])
                },
                "source_table": "percentage_use_by_driver"
            }
        
        # Default factors if no match
        return {
            "assigned_driver": assigned_driver,
            "occasional_driver": occasional_driver,
            "lookup_key": "default",
            "factors": {
                "BIPD": 1.0,
                "COLL": 1.0,
                "COMP": 1.0,
                "MPC": 1.0,
                "U": 1.0
            },
            "source_table": "percentage_use_by_driver"
        }

    def get_years_licensed_factors(self, years_licensed: int, assigned_driver: str) -> Dict[str, Any]:
        """Get years licensed factors."""
        if self._years_licensed is None:
            self._years_licensed = self.data_loader.load_years_licensed_key()
        
        # Determine range and lookup key based on years
        assigned_driver_lookup = assigned_driver
        if assigned_driver == 'No':
            years_licensed_key = 3
            years_licensed_range = "All Not Listed"
        else:  # Assigned driver is 'Yes'
            if years_licensed <= 48:
                years_licensed_key = 1
                years_licensed_range = "0 – 48"
            else:  # years_licensed > 48
                years_licensed_key = 2
                years_licensed_range = "49 – 83"
        
        # Find factors for each coverage type
        factors = {}
        for coverage in ['BIPD', 'COLL', 'COMP', 'MPC', 'U']:
            match = self._years_licensed[
                (self._years_licensed['Coverage'] == coverage) &
                (self._years_licensed['Assigned Driver'] == assigned_driver_lookup) &
                (self._years_licensed['Years Liscensed Key'] == years_licensed_key)
            ]
            
            if not match.empty:
                factors[coverage] = float(match['Factor'].iloc[0])
            else:
                factors[coverage] = 1.0  # Default
        
            return {
            "years_licensed": years_licensed,
            "assigned_driver": assigned_driver,
            "years_licensed_key": years_licensed_key,
            "years_licensed_range": years_licensed_range,
            "factors": factors,
            "source_table": "years_liscensed_key"
        }

    def get_single_auto_factors(self, single_auto: bool) -> Dict[str, Any]:
        """Get single automobile factors."""
        if self._single_auto_factors is None:
            self._single_auto_factors = self.data_loader.load_single_auto_factors()
        
        # If it's not a single auto, return default factors of 1.0
        if not single_auto:
            return {
                "single_automobile": False,
                "factors": {
                    "BIPD": 1.0,
                    "COLL": 1.0,
                    "COMP": 1.0,
                    "MPC": 1.0,
                    "U": 1.0
                },
                "source_table": "single_auto_factor"
            }
        
        # Return the actual single auto factors
        factors = {}
        for _, row in self._single_auto_factors.iterrows():
            coverage = row['coverage']
            factors[coverage] = float(row['single_automobile_factor'])
        
        return {
            "single_automobile": True,
            "factors": factors,
            "source_table": "single_auto_factor"
        }

    def get_safety_factors(self, rate_level: int) -> Dict[str, Any]:
        """Get safety record rating factors for a given rate level."""
        if self._safety_rating_factors is None:
            self._safety_rating_factors = self.data_loader.load_safety_rating_factors()
        
        # Map clean driver (level 0) to neutral factors (level 8)
        lookup_level = rate_level
        
        # Find the row for the lookup level
        match = self._safety_rating_factors[self._safety_rating_factors['rate_level'] == lookup_level]
        
        if match.empty:
            # Default to level 8 (clean record) if not found
            match = self._safety_rating_factors[self._safety_rating_factors['rate_level'] == 0]
        
        if match.empty:
            # Fallback to 1.0 factors if even level 8 is missing
            return {
                "rate_level": rate_level,
                "safety_record_level": rate_level,  # Add this for frontend compatibility
                "factors": {
                    "BIPD": 1.0,
                    "COLL": 1.0,
                    "COMP": 1.0,
                    "MPC": 1.0,
                    "U": 1.0
                },
                "source_table": "driving_safety_record_rating_plan"
            }
        
        row = match.iloc[0]
        return {
            "rate_level": rate_level,
            "safety_record_level": rate_level,  # Add this for frontend compatibility
            "factors": {
                "BIPD": float(row['bipd_factor']),
                "COLL": float(row['coll_factor']),
                "COMP": float(row['comp_factor']),
                "MPC": float(row['mpc_factor']),
                "U": float(row['um_factor'])
            },
            "source_table": "driving_safety_record_rating_plan"
        }

    def get_percentage_use_factors(self, percentage_use: float, assigned_driver: bool) -> Dict[str, Any]:
        """Get percentage use by driver factors."""
        if self._percentage_use is None:
            self._percentage_use = self.data_loader.load_percentage_use_by_driver()
        
        # Determine the lookup key based on assigned driver and percentage
        if assigned_driver:
            if percentage_use == 100.0:
                key = "All Not Specifically Listed, Yes"
            else:
                key = "Yes, No"  # Partial usage
        else:
            key = "No, No"  # Not assigned driver
        
        # Find the row for the key
        if key in self._percentage_use.index:
            row = self._percentage_use.loc[key]
            return {
                "percentage_use": percentage_use,
                "assigned_driver": assigned_driver,
                "lookup_key": key,
                "factors": {
                    "BIPD": float(row['BIPD']),
                    "COLL": float(row['COLL']),
                    "COMP": float(row['COMP']),
                    "MPC": float(row['MPC']),
                    "U": float(row['U'])
                },
                "source_table": "percentage_use_by_driver"
            }
        else:
            # Default to 1.0 if not found
            return {
                "percentage_use": percentage_use,
                "assigned_driver": assigned_driver,
                "lookup_key": key,
                "factors": {
                    "BIPD": 1.0,
                    "COLL": 1.0,
                    "COMP": 1.0,
                    "MPC": 1.0,
                    "U": 1.0
                },
                "source_table": "percentage_use_by_driver"
            }
