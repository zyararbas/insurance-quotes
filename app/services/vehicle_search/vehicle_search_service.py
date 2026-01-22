import logging 
import requests
from typing import Dict, Any, List, Optional
from app.services.lookup_services.vehicle_lookup_service import VehicleLookupService
from datetime import datetime
import math

logger = logging.getLogger(__name__)
def round_up(num, dec=0):
    """
    Rounds a number up to a specified number of decimal places.
    """
    if dec < 0:
        raise ValueError("Decimal places (dec) must be non-negative.")
    mult = 10**dec
    return math.ceil(num * mult) / mult

class VehicleSearchService:
    """
    Vehicle search service with built-in VIN lookup.
    
    This service provides a unified interface for vehicle search that handles:
    1. VIN-based searches (automatic lookup + search)
    2. Manual searches (direct criteria)
    3. Hybrid searches (VIN + additional criteria)
    """
    
    def __init__(self):
        """Initialize the vehicle search service."""
        self.vehicle_lookup_service = VehicleLookupService()
        self.vin_base_url = "https://vpic.nhtsa.dot.gov/api/vehicles"
        self.vin_session = requests.Session()
        self.vin_session.headers.update({
            'User-Agent': 'Insurance-Quotes-Service/1.0'
        })
        self._initialized = False
    
    def initialize(self):
        """Initialize the underlying services."""
        if not self._initialized:
            self._initialized = True
            logger.info("VehicleSearchService initialized")
    
    def search_vehicles(
        self,
        vin: Optional[str] = None,
        make: Optional[str] = None,
        model: Optional[str] = None,
        year: Optional[int] = None,
        series: Optional[str] = None,
        package: Optional[str] = None,
        style: Optional[str] = None,
        engine: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Search for vehicles with built-in VIN lookup support.
        
        This method handles both VIN-based and manual vehicle searches:
        - If VIN provided: Decode VIN → extract criteria → search database
        - If manual criteria: Search database directly
        - If both: VIN provides base criteria, manual overrides specific fields
        
        Args:
            vin: Vehicle Identification Number (optional)
            make: Vehicle make (e.g., "BMW", "Toyota")
            model: Vehicle model (e.g., "X3", "Camry")
            year: Vehicle year (e.g., 2020)
            series: Vehicle series (e.g., "XDRIVE 30I")
            package: Vehicle package (e.g., "CONVENIENCE")
            style: Vehicle style (e.g., "AWD 4D")
            engine: Engine type (e.g., "I4")
            
        Returns:
            Dict containing search results, VIN data (if applicable), and metadata
        """
        if not self._initialized:
            self.initialize()
        
        try:
            # Step 1: Handle VIN lookup if provided
            vin_data = None
            if vin:
                try:
                    vin_data = self._lookup_vin(vin)
                    logger.info(f"VIN lookup successful: {vin_data.get('make')} {vin_data.get('model')} {vin_data.get('year')}")
                except Exception as e:
                    logger.warning(f"VIN lookup failed: {e}")
                    # Continue with manual criteria if VIN fails
                    vin_data = None
            
            # Step 2: Determine search criteria
            search_criteria = self._determine_search_criteria(vin_data, make, model, year)
            # Step 3: Validate search criteria
            if not self._validate_search_criteria(search_criteria):
                return {
                    'error': 'Missing required search criteria. Please provide make, model, and year.',
                    'status': 'incomplete_criteria',
                    'vin_data': vin_data
                }
            
            # Step 4: Perform vehicle search
            search_results = self._perform_vehicle_search(search_criteria, series, package, style, engine)
            
            # Step 5: Format and return results
            return self._format_search_results(
                vin_data, search_criteria, search_results, vin
            )
            
        except Exception as e:
            logger.error(f"Vehicle search failed: {e}")
            return {
                'error': f'Vehicle search failed: {str(e)}',
                'status': 'search_error',
                'vin_data': vin_data if 'vin_data' in locals() else None
            }
    
    def _lookup_vin(self, vin: str) -> Dict[str, Any]:
        """Look up vehicle information using a VIN."""
        if not vin or len(vin.strip()) != 17:
            raise ValueError("VIN must be exactly 17 characters long")
        
        vin = vin.strip().upper()
        
        try:
            # Make request to NHTSA vPIC API
            response = self.vin_session.get(
                f"{self.vin_base_url}/decodevin/{vin}",
                params={'format': 'json'},
                timeout=120
            )
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('Results'):
                return self._parse_vin_data(data)
            else:
                raise ValueError(f"VIN lookup failed: {data.get('Message', 'Unknown error')}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"VIN lookup request failed: {e}")
            raise ValueError(f"VIN lookup service unavailable: {str(e)}")
        except Exception as e:
            logger.error(f"VIN lookup failed: {e}")
            raise ValueError(f"VIN lookup failed: {str(e)}")
    
    def _parse_vin_data(self, api_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse VIN data from NHTSA API response."""
        results = api_data.get('Results', [])
        if not results:
            raise ValueError("No vehicle data found for VIN")
        
        # Convert results array to dictionary for easier access
        vehicle_data = {}
        for result in results:
            variable = result.get('Variable', '')
            value = result.get('Value', '')
            if variable and value and value != 'None':
                # Normalize field names
                normalized_name = variable.lower().replace(' ', '_').replace('-', '_')
                vehicle_data[normalized_name] = value
        
        # Parse the vehicle information using the correct field names
        parsed_data = {
            'vin': vehicle_data.get('suggested_vin', ''),
            'make': self._clean_text(vehicle_data.get('make', '')),
            'model': self._clean_text(vehicle_data.get('model', '')),
            'year': self._parse_year(vehicle_data.get('model_year', '')),
            'series': self._clean_text(vehicle_data.get('series', '')),
            'trim': self._clean_text(vehicle_data.get('trim', '')), 
            'body_class': self._clean_text(vehicle_data.get('body_class', '')),
            'drive_type': self._clean_text(vehicle_data.get('drive_type', '')),
            'fuel_type': self._clean_text(vehicle_data.get('fuel_type___primary', '')),
            'engine_config': self._clean_text(vehicle_data.get('engine_configuration', '')),
            'transmission': self._clean_text(vehicle_data.get('transmission_style', '')),
            'doors': self._parse_number(vehicle_data.get('doors', '')),
            'windows': self._parse_number(vehicle_data.get('windows', '')),
            'wheels': self._parse_number(vehicle_data.get('wheels', '')),
            'gvwr': self._clean_text(vehicle_data.get('gvwr', '')),
        }
        displacement = self._clean_text(vehicle_data.get('displacement_(l)', ''))
        if displacement and displacement != '':
            try:
                parsed_data['engine_displacement_(l)'] = round_up(float(displacement), 1)
            except ValueError:
                logger.error(f"Invalid displacement value: {displacement}")
        # Remove empty values
        parsed_data = {k: v for k, v in parsed_data.items() if v}
        
        return parsed_data
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text data."""
        if not text or text.strip() == '':
            return ''
        return text.strip()
    
    def _parse_year(self, year_str: str) -> Optional[int]:
        """Parse year from string."""
        try:
            if year_str and year_str.strip():
                return int(year_str.strip())
        except (ValueError, TypeError):
            pass
        return None
    
    def _parse_number(self, num_str: str) -> Optional[int]:
        """Parse number from string."""
        try:
            if num_str and num_str.strip():
                return int(num_str.strip())
        except (ValueError, TypeError):
            pass
        return None
    
    def _determine_search_criteria(
        self, 
        vin_data: Optional[Dict[str, Any]], 
        make: Optional[str], 
        model: Optional[str], 
        year: Optional[int]
    ) -> Dict[str, Any]:
        """Determine search criteria from VIN data or manual input."""
        criteria = {}
        
        if vin_data:
            # Use VIN data as primary source
            criteria.update({
                'make': vin_data.get('make'),
                'model': vin_data.get('model'),
                'year': vin_data.get('year')
            })
        
        # Override with manual input if provided
        if make:
            criteria['make'] = make
        if model:
            criteria['model'] = model
        if year:
            criteria['year'] = year
        
        return criteria
    
    def _validate_search_criteria(self, criteria: Dict[str, Any]) -> bool:
        """Validate that required search criteria are present."""
        required_fields = ['make', 'model', 'year']
        return all(criteria.get(field) for field in required_fields)
    
    def _perform_vehicle_search(
        self, 
        criteria: Dict[str, Any], 
        series: Optional[str] = None,
        package: Optional[str] = None,
        style: Optional[str] = None,
        engine: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Perform vehicle search using the lookup service."""
        # Get base search results
        search_results = self.vehicle_lookup_service.search_vehicles(
            make=criteria.get('make'),
            model=criteria.get('model'),
            year=criteria.get('year')
        )
        
        # Apply additional filters if provided
        if series or package or style or engine:
            search_results = self._filter_results(
                search_results, series, package, style, engine
            )
        
        return search_results
    
    def _filter_results(
        self,
        results: List[Dict[str, Any]],
        series: Optional[str] = None,
        package: Optional[str] = None,
        style: Optional[str] = None,
        engine: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Filter search results based on additional criteria."""
        filtered_results = []
        
        for vehicle in results:
            matches = True
            
            if series and vehicle.get('series', '').upper() != series.upper():
                matches = False
            
            if package and vehicle.get('package', '').upper() != package.upper():
                matches = False
            
            if style and vehicle.get('style', '').upper() != style.upper():
                matches = False
            
            if engine and vehicle.get('engine', '').upper() != engine.upper():
                matches = False
            
            if matches:
                filtered_results.append(vehicle)
        
        return filtered_results
    
    def _format_search_results(
        self,
        vin_data: Optional[Dict[str, Any]],
        search_criteria: Dict[str, Any],
        search_results: List[Dict[str, Any]],
        vin: Optional[str]
    ) -> Dict[str, Any]:
        """Format and return search results with metadata."""
        result = {
            'status': 'success',
            'search_criteria': search_criteria,
            'total_matches': len(search_results),
            'vehicles': search_results
        }
        
        # Add VIN data if available
        if vin_data:
            result['vin_data'] = vin_data
            result['search_type'] = 'vin_based'
        else:
            result['search_type'] = 'manual'
        
        # Add VIN if provided (even if lookup failed)
        if vin:
            result['vin_provided'] = vin
        
        logger.info(f"Vehicle search completed: {len(search_results)} matches found")
        return result
    
    def get_available_options(
        self,
        vin: Optional[str] = None,
        make: Optional[str] = None,
        model: Optional[str] = None,
        year: Optional[int] = None,
        option_type: str = 'series'
    ) -> List[str]:
        """
        Get available options for a specific vehicle with VIN support.
        
        Args:
            vin: Vehicle Identification Number (optional)
            make: Vehicle make (optional if VIN provided)
            model: Vehicle model (optional if VIN provided)
            year: Vehicle year (optional if VIN provided)
            option_type: Type of options to retrieve (series, package, style, engine)
            
        Returns:
            List of available options
        """
        if not self._initialized:
            self.initialize()
        
        try:
            # Handle VIN lookup if provided
            if vin:
                try:
                    vin_data = self._lookup_vin(vin)
                    make = make or vin_data.get('make')
                    model = model or vin_data.get('model')
                    year = year or vin_data.get('year')
                except Exception as e:
                    logger.warning(f"VIN lookup failed for options: {e}")
                    # Continue with manual criteria
            
            # Validate required criteria
            if not all([make, model, year]):
                logger.error("Missing required criteria for options lookup")
                return []
            
            # Get all vehicles for the make/model/year
            vehicles = self.vehicle_lookup_service.search_vehicles(
                make=make, model=model, year=year
            )
            
            # Extract unique options
            options = set()
            for vehicle in vehicles:
                option_value = vehicle.get(option_type, '')
                if option_value and option_value.strip():
                    options.add(option_value.strip())
            
            return sorted(list(options))
            
        except Exception as e:
            logger.error(f"Failed to get available options: {e}")
            return []
    
    def search_by_vin_only(self, vin: str) -> Dict[str, Any]:
        """
        Simplified method for VIN-only searches.
        
        Args:
            vin: Vehicle Identification Number
            
        Returns:
            Dict containing search results and VIN data
        """
        return self.search_vehicles(vin=vin)
    
    def search_by_criteria_only(
        self,
        make: str,
        model: str,
        year: int,
        series: Optional[str] = None,
        package: Optional[str] = None,
        style: Optional[str] = None,
        engine: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Simplified method for criteria-only searches.
        
        Args:
            make: Vehicle make
            model: Vehicle model
            year: Vehicle year
            series: Vehicle series (optional)
            package: Vehicle package (optional)
            style: Vehicle style (optional)
            engine: Engine type (optional)
            
        Returns:
            Dict containing search results
        """
        return self.search_vehicles(
            make=make, model=model, year=year,
            series=series, package=package, style=style, engine=engine
        )
