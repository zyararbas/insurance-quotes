import pandas as pd
import logging
from typing import Dict, List, Optional
from utils.data_loader import DataLoader
from models.models import Coverages

logger = logging.getLogger(__name__)

class CoverageFactorLookupService:
    """
    Microservice for coverage factor lookups.
    Handles coverage limits, deductibles, and their associated factors.
    """
    
    def __init__(self, carrier_config: dict):
        self.carrier_config = carrier_config
        self.data_loader = DataLoader()
        
        # Coverage factor tables
        self.bi_limits: pd.DataFrame = None
        self.pd_limits: pd.DataFrame = None
        self.um_limits: pd.DataFrame = None
        self.mpc_limits: pd.DataFrame = None
        self.drg_deductible_factors: pd.DataFrame = None
        self.grg_deductible_factors: pd.DataFrame = None
        
    def initialize(self):
        """Loads all coverage factor data tables."""
        self.bi_limits = self.data_loader.load_bi_limits()
        self.pd_limits = self.data_loader.load_pd_limits()
        self.um_limits = self.data_loader.load_um_limits()
        self.mpc_limits = self.data_loader.load_mpc_limits()
        self.drg_deductible_factors = self.data_loader.load_drg_deductible_factors()
        self.grg_deductible_factors = self.data_loader.load_grg_deductible_factors()
        
        logger.info("CoverageFactorLookupService initialized")
        
    def get_all_coverage_limits(self) -> dict:
        """
        Returns all available coverage limits and deductible options for the frontend dropdowns.
        """
        if self.bi_limits is None:
            self.initialize()
            
        # Get BI limits (BIPD uses these)
        bipd_limits = []
        for _, row in self.bi_limits.iterrows():
            bipd_limits.append({
                'value': row['bi_limits'],
                'label': f"{row['bi_limits']}k"
            })
        
        # Get PD limits
        pd_limits = []
        for _, row in self.pd_limits.iterrows():
            pd_limits.append({
                'value': str(int(row['limit'] * 1000)),  # Convert to thousands
                'label': f"{int(row['limit'])}k"
            })
        
        # Get UM limits
        um_limits = []
        for _, row in self.um_limits.iterrows():
            um_limits.append({
                'value': row['limits'],
                'label': f"{row['limits']}k"
            })
        
        # Get MPC limits
        mpc_limits = []
        for _, row in self.mpc_limits.iterrows():
            mpc_limits.append({
                'value': str(int(row['limit'])),
                'label': f"${int(row['limit'])}"
            })
        
        # Get collision deductibles (from DRG table headers)
        collision_deductibles = [
            {'value': 'FULL', 'label': 'Full Coverage'},
            {'value': '50', 'label': '$50'},
            {'value': '100', 'label': '$100'},
            {'value': '200', 'label': '$200'},
            {'value': '250', 'label': '$250'},
            {'value': '500', 'label': '$500'},
            {'value': '1000', 'label': '$1000'},
            {'value': '1000W/20%', 'label': '$1000 w/20%'},
            {'value': '2000', 'label': '$2000'}
        ]
        
        # Get comprehensive deductibles (from GRG table headers)
        comprehensive_deductibles = [
            {'value': '50', 'label': '$50'},
            {'value': '100', 'label': '$100'},
            {'value': '200', 'label': '$200'},
            {'value': '250', 'label': '$250'},
            {'value': '500', 'label': '$500'},
            {'value': '1000', 'label': '$1000'},
            {'value': '1000 W/20%', 'label': '$1000 w/20%'},
            {'value': '2000', 'label': '$2000'}
        ]
        
        # Multi-policy options (hardcoded for now)
        multi_policy_options = [
            {'value': 'home', 'label': 'Home'},
            {'value': 'renters', 'label': 'Renters'},
            {'value': 'condo', 'label': 'Condo'},
            {'value': 'none', 'label': 'None'}
        ]
        
        return {
            'bipdLimits': bipd_limits,
            'pdLimits': pd_limits,
            'umLimits': um_limits,
            'mpcLimits': mpc_limits,
            'collisionDeductibles': collision_deductibles,
            'comprehensiveDeductibles': comprehensive_deductibles,
            'multiPolicyOptions': multi_policy_options
        }
        
    def get_bi_factor(self, coverage: str, limit: str) -> float:
        """Gets the BIPD factor for a specific limit."""
        if self.bi_limits is None:
            self.initialize()
            
        try:
            row = self.bi_limits[self.bi_limits['bi_limits'] == limit]
            if not row.empty:
                factor = float(row['factor'].iloc[0])
                logger.info(f"BIPD factor for limit {limit}: {factor}")
                return factor
            else:
                logger.warning(f"No BIPD factor found for limit {limit}")
                return 1.0
        except Exception as e:
            logger.error(f"Error getting BIPD factor for limit {limit}: {e}")
            return 1.0
            
    def get_pd_factor(self, coverage: str, limit: str) -> float:
        """Gets the PD factor for a specific limit."""
        if self.pd_limits is None:
            self.initialize()
            
        try:
            limit_int = int(limit)
            # The CSV table already has values in thousands, so use the limit directly
            row = self.pd_limits[self.pd_limits['limit'] == limit_int]
            if not row.empty:
                factor = float(row['factor'].iloc[0])
                logger.info(f"PD factor for limit {limit}: {factor}")
                return factor
            else:
                logger.warning(f"No PD factor found for limit {limit}")
                return 1.0
        except Exception as e:
            logger.error(f"Error getting PD factor for limit {limit}: {e}")
            return 1.0
            
    def get_um_factor(self, coverage: str, limit: str) -> float:
        """Gets the UM factor for a specific limit."""
        if self.um_limits is None:
            self.initialize()
            
        try:
            row = self.um_limits[self.um_limits['limits'] == limit]
            if not row.empty:
                factor = float(row['factor'].iloc[0])
                logger.info(f"UM factor for limit {limit}: {factor}")
                return factor
            else:
                logger.warning(f"No UM factor found for limit {limit}")
                return 1.0
        except Exception as e:
            logger.error(f"Error getting UM factor for limit {limit}: {e}")
            return 1.0
            
    def get_mpc_factor(self, coverage: str, limit: str) -> float:
        """Gets the MPC factor for a specific limit."""
        if self.mpc_limits is None:
            self.initialize()
            
        try:
            limit_int = int(limit)
            row = self.mpc_limits[self.mpc_limits['limit'] == limit_int]
            if not row.empty:
                factor = float(row['factor'].iloc[0])
                logger.info(f"MPC factor for limit {limit}: {factor}")
                return factor
            else:
                logger.warning(f"No MPC factor found for limit {limit}")
                return 1.0
        except Exception as e:
            logger.error(f"Error getting MPC factor for limit {limit}: {e}")
            return 1.0
            
    def get_collision_factor(self, coverage: str, deductible: str, drg: int) -> float:
        """Gets the collision factor for a specific deductible and DRG."""
        if self.drg_deductible_factors is None:
            self.initialize()
            
        try:
            factor = self.drg_deductible_factors.loc[drg, deductible]
            logger.info(f"Collision factor for deductible {deductible} at DRG {drg}: {factor}")
            return float(factor)
        except (KeyError, IndexError):
            logger.warning(f"No collision factor found for deductible {deductible} at DRG {drg}")
            return 1.0
            
    def get_comprehensive_factor(self, coverage: str, deductible: str, grg: int) -> float:
        """Gets the comprehensive factor for a specific deductible and GRG."""
        if self.grg_deductible_factors is None:
            self.initialize()
            
        try:
            factor = self.grg_deductible_factors.loc[grg, deductible]
            logger.info(f"Comprehensive factor for deductible {deductible} at GRG {grg}: {factor}")
            return float(factor)
        except (KeyError, IndexError):
            logger.warning(f"No comprehensive factor found for deductible {deductible} at GRG {grg}")
            return 1.0
            
    def get_coverage_factor(self, coverage: str, limit_or_deductible: str, 
                           vehicle_rating_group: Optional[int] = None) -> float:
        """
        Gets the coverage factor for a specific coverage type, limit/deductible, and vehicle rating group.
        """
        if coverage == "BIPD":
            # BIPD = BI factor + PD factor
            bi_factor = self.get_bi_factor(coverage, limit_or_deductible)
            # Extract PD limit from BIPD limit (e.g., "250/500" -> "250")
            pd_limit = limit_or_deductible.split('/')[0] if '/' in limit_or_deductible else limit_or_deductible
            pd_factor = self.get_pd_factor(coverage, pd_limit)
            bipd_factor = bi_factor + pd_factor
            logger.info(f"BIPD factor calculation: BI({limit_or_deductible})={bi_factor} + PD({pd_limit})={pd_factor} = {bipd_factor}")
            return bipd_factor
        elif coverage == "PD":
            return self.get_pd_factor(coverage, limit_or_deductible)
        elif coverage == "UM":
            return self.get_um_factor(coverage, limit_or_deductible)
        elif coverage == "MPC":
            return self.get_mpc_factor(coverage, limit_or_deductible)
        elif coverage == "COLL" and vehicle_rating_group is not None:
            return self.get_collision_factor(coverage, limit_or_deductible, vehicle_rating_group)
        elif coverage == "COMP" and vehicle_rating_group is not None:
            return self.get_comprehensive_factor(coverage, limit_or_deductible, vehicle_rating_group)
        else:
            logger.warning(f"Unsupported coverage type: {coverage}")
            return 1.0
            
    def calculate_coverage_factors(self, coverages: Coverages, vehicle_rating_groups: Dict) -> Dict:
        """
        Calculates coverage factors for all selected coverages.
        Returns: {coverage: {'factor': float, 'limit_or_deductible': str, 'rating_group': int}}
        """
        if self.bi_limits is None:
            self.initialize()
            
        results = {}
        
        for coverage_code, coverage_details in coverages.dict().items():
            if not coverage_details.get('selected'):
                continue
                
            # Get the limit or deductible
            limit_or_deductible = None
            if coverage_details.get('deductible') is not None:
                limit_or_deductible = str(coverage_details.get('deductible'))
            elif coverage_details.get('limits') is not None:
                limit_or_deductible = str(coverage_details.get('limits'))
                
            if limit_or_deductible is None:
                logger.warning(f"No limit or deductible found for {coverage_code}")
                continue
                
            # Get the appropriate rating group
            rating_group = None
            if coverage_code == "COLL":
                rating_group = vehicle_rating_groups.get('drg')
            elif coverage_code == "COMP":
                rating_group = vehicle_rating_groups.get('grg')
                
            # Get the coverage factor
            factor = self.get_coverage_factor(coverage_code, limit_or_deductible, rating_group)
            
            results[coverage_code] = {
                'factor': factor,
                'limit_or_deductible': limit_or_deductible,
                'rating_group': rating_group
            }
            
            logger.info(f"Coverage factor for {coverage_code}: {factor}")
            
        return results
