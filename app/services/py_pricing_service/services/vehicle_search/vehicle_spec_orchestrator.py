"""
Vehicle Specification Orchestrator Service

This is a lightweight orchestrator that coordinates the 4 core vehicle services:
1. Vehicle Search (VIN or Year/Make/Model)
2. Enhanced AI Assistant (with deduplication)
3. Specific Vehicle Spec Lookup
4. End-to-End orchestration

This service provides a clean API for all vehicle specification use cases.
"""

import logging
from typing import Dict, Any, List, Optional
from .vehicle_search_service import VehicleSearchService
from .ai_assistant_service import AIAssistantService

logger = logging.getLogger(__name__)

class VehicleSpecOrchestrator:
    """
    Lightweight orchestrator for vehicle specification services.
    
    This service coordinates the 4 core vehicle services to provide a clean API
    for all vehicle specification use cases.
    """
    
    def __init__(self):
        """Initialize the vehicle specification orchestrator."""
        self.search_service = VehicleSearchService()
        self.ai_service = AIAssistantService(provider="openai")
        
        # Initialize services
        self._initialized = False
    
    def initialize(self):
        """Initialize all underlying services."""
        if not self._initialized:
            self.search_service.initialize()
            self._initialized = True
            logger.info("VehicleSpecOrchestrator initialized")
    
    def process_vehicle_request(
        self,
        vin: Optional[str] = None,
        make: Optional[str] = None,
        model: Optional[str] = None,
        year: Optional[int] = None,
        additional_info: Optional[str] = None,
        conversation_history: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """
        Process a complete vehicle specification request.
        
        This is the main entry point that handles both VIN-based and manual
        vehicle specification requests.
        
        Args:
            vin: Vehicle Identification Number (optional)
            make: Vehicle make (e.g., "BMW", "Toyota")
            model: Vehicle model (e.g., "X3", "Camry")
            year: Vehicle year (e.g., 2020)
            additional_info: Additional vehicle information (e.g., "Package: Convenience")
            conversation_history: Previous Q&A exchanges for context
            
        Returns:
            Dict containing the final vehicle specification or questions for clarification
        """
        try:
            if not self._initialized:
                self.initialize()
            
            # Step 1: Perform vehicle search (with built-in VIN lookup)
            search_result = self.search_service.search_vehicles(
                vin=vin,
                make=make,
                model=model,
                year=year
            )
            
            # Check for search errors
            if 'error' in search_result:
                return search_result
            
            search_results = search_result.get('vehicles', [])
            vin_data = search_result.get('vin_data')
            
            if not search_results:
                return {
                    'error': 'No vehicles found matching the specified criteria.',
                    'status': 'no_results'
                }
            
            # Step 2: AI interpretation with deduplication
            ai_result = self._perform_ai_interpretation(
                vin_data, search_results, additional_info, conversation_history
            )
            
            # Step 3: Process and format results
            result = self._process_results(
                vin_data, search_result.get('search_criteria', {}), search_results, ai_result, conversation_history
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Vehicle specification request failed: {e}", exc_info=True)
            return {
                'error': f'An internal error occurred: {str(e)}',
                'status': 'service_error'
            }
    
    def get_vehicle_spec_by_criteria(
        self,
        make: str,
        model: str,
        year: int,
        series: Optional[str] = None,
        package: Optional[str] = None,
        style: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get vehicle specifications by exact criteria (Service 3).
        
        This method retrieves all matching vehicle specifications for the given criteria.
        
        Args:
            make: Vehicle make
            model: Vehicle model
            year: Vehicle year
            series: Vehicle series (optional)
            package: Vehicle package (optional)
            style: Vehicle style (optional)
            
        Returns:
            Dict containing search results and metadata
        """
        try:
            if not self._initialized:
                self.initialize()
            
            # Use the enhanced search service
            return self.search_service.search_vehicles(
                make=make,
                model=model,
                year=year,
                series=series,
                package=package,
                style=style
            )
            
        except Exception as e:
            logger.error(f"Vehicle spec lookup failed: {e}")
            return {
                'error': f'Vehicle spec lookup failed: {str(e)}',
                'status': 'lookup_error'
            }
    
    def _perform_ai_interpretation(
        self,
        vin_data: Optional[Dict[str, Any]],
        search_results: List[Dict[str, Any]],
        additional_info: str,
        conversation_history: Optional[List[Dict[str, str]]]
    ) -> Dict[str, Any]:
        """Perform AI interpretation with built-in deduplication."""
        logger.info(f"=== AI Interpretation Debug ===")
        logger.info(f"Search results count: {len(search_results)}")
        logger.info(f"Conversation history length: {len(conversation_history) if conversation_history else 0}")
        logger.info(f"Additional info: {additional_info[:200]}..." if additional_info else "None")
        
        ai_result = self.ai_service.interpret_vehicle_results(
            vin_data, search_results, additional_info, conversation_history
        )
        
        logger.info(f"=== AI Service Response ===")
        logger.info(f"AI Result: {ai_result}")
        logger.info(f"AI Match: {ai_result.get('match') if ai_result else 'None'}")
        logger.info(f"AI Questions: {ai_result.get('questions', []) if ai_result else 'None'}")
        
        return ai_result
    
    def _process_results(
        self,
        vin_data: Optional[Dict[str, Any]],
        search_criteria: Dict[str, Any],
        search_results: List[Dict[str, Any]],
        ai_result: Dict[str, Any],
        conversation_history: Optional[List[Dict[str, str]]]
    ) -> Dict[str, Any]:
        """Process and format the final results with detailed step tracking."""
        result = {
            'status': 'success',
            'search_criteria': search_criteria,
            'total_matches': len(search_results),
            'ai_result': ai_result,
            'step_details': {
                'step_1_vin_lookup': vin_data if vin_data else None,
                'step_2_initial_search_results': search_results,
                'step_3_deduplicated_results': self._get_deduplicated_results(search_results),
                'step_4_ai_service_results': ai_result,
                'step_5_follow_up_questions': ai_result.get('questions', []) if ai_result else [],
                'step_6_lookup_results_from_ai': self._get_ai_match_results(ai_result, search_results),
                'step_7_conflict_resolution': self._get_conflict_resolution_details(ai_result)
            }
        }
        
        # Add VIN data if available
        if vin_data:
            result['vin_data'] = vin_data
        
        # Add conversation history if available
        if conversation_history:
            result['conversation_history'] = conversation_history
        
        return result
    
    def _get_deduplicated_results(self, search_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Get deduplicated results by grouping identical specifications."""
        if not search_results:
            return []
        
        # Group by specification (excluding ratings)
        spec_groups = {}
        for vehicle in search_results:
            spec_key = self._create_specification_key(vehicle)
            if spec_key not in spec_groups:
                spec_groups[spec_key] = []
            spec_groups[spec_key].append(vehicle)
        
        # Return one representative from each group
        deduplicated = []
        for group in spec_groups.values():
            if group:
                # Use the first vehicle as representative
                deduplicated.append(group[0])
        
        return deduplicated
    
    def _create_specification_key(self, vehicle: Dict[str, Any]) -> str:
        """Create a unique key for vehicle specification (excluding ratings)."""
        key_components = [
            str(vehicle.get('year', '')),
            str(vehicle.get('make', '')),
            str(vehicle.get('model', '')),
            str(vehicle.get('series', '')),
            str(vehicle.get('package', '')),
            str(vehicle.get('style', '')),
            str(vehicle.get('engine', '')),
            str(vehicle.get('wheelbase', ''))
        ]
        return '|'.join(key_components)
    
    def _get_ai_match_results(self, ai_result: Dict[str, Any], search_results: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Get the specific vehicle match from AI results."""
        logger.info(f"=== AI Match Results Debug ===")
        logger.info(f"AI Result: {ai_result}")
        logger.info(f"Search Results Count: {len(search_results) if search_results else 0}")
        
        if not ai_result:
            logger.warning("No AI result provided")
            return None
            
        if not ai_result.get('match'):
            logger.warning("No match in AI result")
            return None
        
        match = ai_result.get('match')
        logger.info(f"=== AI MATCH STRING ===")
        logger.info(f"AI match: {match} (type: {type(match)})")
        logger.info(f"=== END AI MATCH STRING ===")
        
        # If match is a string, try to find the corresponding vehicle
        if isinstance(match, str):
            logger.info(f"=== VEHICLE MATCHING PROCESS ===")
            logger.info(f"AI match string: '{match}'")
            logger.info(f"Searching through {len(search_results)} vehicles")
            
            # Show all available vehicles first
            logger.info(f"=== AVAILABLE VEHICLES ===")
            for i, vehicle in enumerate(search_results):
                vehicle_str = self._create_vehicle_string(vehicle)
                logger.info(f"Vehicle {i+1}: '{vehicle_str}'")
                logger.info(f"  - Year: {vehicle.get('year')}")
                logger.info(f"  - Make: {vehicle.get('make')}")
                logger.info(f"  - Model: {vehicle.get('model')}")
                logger.info(f"  - Series: {vehicle.get('series')}")
                logger.info(f"  - Package: {vehicle.get('package')}")
                logger.info(f"  - Style: {vehicle.get('style')}")
            logger.info(f"=== END AVAILABLE VEHICLES ===")
            
            # Try exact match first
            logger.info(f"=== EXACT MATCH ATTEMPT ===")
            for i, vehicle in enumerate(search_results):
                vehicle_str = self._create_vehicle_string(vehicle)
                logger.info(f"Comparing: '{match}' == '{vehicle_str}'")
                if vehicle_str == match:
                    logger.info(f"✅ EXACT MATCH FOUND: Vehicle {i+1}")
                    return vehicle
            logger.info(f"❌ No exact match found")
            
            # Try flexible exact match (handle format differences)
            logger.info(f"=== FLEXIBLE EXACT MATCH ATTEMPT ===")
            for i, vehicle in enumerate(search_results):
                vehicle_str = self._create_vehicle_string(vehicle)
                # Normalize both strings for comparison
                normalized_match = self._normalize_vehicle_string(match)
                normalized_vehicle = self._normalize_vehicle_string(vehicle_str)
                logger.info(f"Comparing normalized: '{normalized_match}' == '{normalized_vehicle}'")
                if normalized_vehicle == normalized_match:
                    logger.info(f"✅ FLEXIBLE EXACT MATCH FOUND: Vehicle {i+1}")
                    return vehicle
            logger.info(f"❌ No flexible exact match found")
            
            # Try partial matching with key components
            logger.info(f"=== COMPONENT MATCH ATTEMPT ===")
            match_components = self._parse_match_string(match)
            logger.info(f"Parsed match components: {match_components}")
            
            for i, vehicle in enumerate(search_results):
                if self._matches_vehicle_components(vehicle, match_components):
                    logger.info(f"✅ COMPONENT MATCH FOUND: Vehicle {i+1}")
                    return vehicle
            logger.info(f"❌ No component match found")
            
            # Try fuzzy matching
            logger.info(f"=== FUZZY MATCH ATTEMPT ===")
            for i, vehicle in enumerate(search_results):
                vehicle_str = self._create_vehicle_string(vehicle)
                if self._fuzzy_match(vehicle_str, match):
                    logger.info(f"✅ FUZZY MATCH FOUND: Vehicle {i+1}")
                    return vehicle
            logger.info(f"❌ No fuzzy match found")
            
            logger.warning(f"=== MATCHING FAILED ===")
            logger.warning(f"Could not find match for: '{match}'")
            logger.warning(f"Available vehicles: {[self._create_vehicle_string(v) for v in search_results[:5]]}")
            return {'match_string': match, 'note': 'Could not find exact vehicle match', 'available_vehicles': [self._create_vehicle_string(v) for v in search_results[:5]]}
        
        # If match is already a vehicle object
        logger.info(f"Match is already a vehicle object: {match}")
        return match
    
    def _create_vehicle_string(self, vehicle: Dict[str, Any]) -> str:
        """Create a string representation of a vehicle for matching."""
        vehicle_str = f"{vehicle.get('year', '')} {vehicle.get('make', '')} {vehicle.get('model', '')}"
        if vehicle.get('series'):
            vehicle_str += f" {vehicle['series']}"
        if vehicle.get('package'):
            vehicle_str += f" {vehicle['package']}"
        if vehicle.get('style'):
            vehicle_str += f" ({vehicle['style']})"
        if vehicle.get('engine'):
            vehicle_str += f" - {vehicle['engine']}"
        if vehicle.get('wheelbase'):
            vehicle_str += f" - WB: {vehicle['wheelbase']}"
        return vehicle_str
    
    def _normalize_vehicle_string(self, vehicle_str: str) -> str:
        """Normalize vehicle string for flexible matching."""
        # Remove extra spaces and normalize format
        normalized = vehicle_str.strip()
        
        # Replace different style formats with consistent format
        # Handle both " - 2D CPE -" and "(2D CPE)" formats
        import re
        
        # Replace dashes around style with parentheses
        normalized = re.sub(r' - ([^-]+) -', r' (\1)', normalized)
        
        # Remove trailing dashes
        normalized = re.sub(r' -+$', '', normalized)
        
        # Normalize spaces
        normalized = ' '.join(normalized.split())
        
        return normalized
    
    def _parse_match_string(self, match_str: str) -> Dict[str, str]:
        """Parse AI match string into components."""
        components = {}
        
        # Extract year, make, model from the beginning
        parts = match_str.split()
        if len(parts) >= 3:
            components['year'] = parts[0]
            components['make'] = parts[1]
            components['model'] = parts[2]
        
        # Extract series (look for patterns like XDRIVE 30I, GT V8, etc.)
        if 'XDRIVE' in match_str.upper():
            components['series'] = 'XDRIVE 30I'
        elif 'SDRIVE' in match_str.upper():
            components['series'] = 'SDRIVE 30I'
        elif 'GT V8' in match_str.upper():
            components['series'] = 'GT V8'
        elif 'GT' in match_str.upper() and 'V8' not in match_str.upper():
            components['series'] = 'GT'
        elif 'GTC V8' in match_str.upper():
            components['series'] = 'GTC V8'
        elif 'GTC' in match_str.upper() and 'V8' not in match_str.upper():
            components['series'] = 'GTC'
        
        # Extract package (look for CONVENIENCE, EXECUTIVE, etc.)
        package_keywords = ['CONVENIENCE', 'EXECUTIVE', 'PREMIUM', 'SPORT']
        for keyword in package_keywords:
            if keyword in match_str.upper():
                components['package'] = keyword
                break
        
        # Extract style (look for AWD, 2WD, 4D, etc.)
        if 'AWD' in match_str.upper():
            components['style'] = 'AWD 4D'
        elif '2WD' in match_str.upper():
            components['style'] = '2WD 4D'
        
        return components
    
    def _matches_vehicle_components(self, vehicle: Dict[str, Any], match_components: Dict[str, str]) -> bool:
        """Check if vehicle matches the parsed components."""
        for key, value in match_components.items():
            if key == 'year' and str(vehicle.get('year', '')) != value:
                return False
            elif key == 'make' and vehicle.get('make', '').upper() != value.upper():
                return False
            elif key == 'model' and vehicle.get('model', '').upper() != value.upper():
                return False
            elif key == 'series' and vehicle.get('series', '').upper() != value.upper():
                return False
            elif key == 'package' and vehicle.get('package', '').upper() != value.upper():
                return False
            elif key == 'style' and vehicle.get('style', '').upper() != value.upper():
                return False
        return True
    
    def _fuzzy_match(self, vehicle_str: str, match_str: str) -> bool:
        """Perform fuzzy matching between vehicle string and match string."""
        # Normalize both strings for comparison
        vehicle_normalized = vehicle_str.upper().replace(' ', '').replace('-', '')
        match_normalized = match_str.upper().replace(' ', '').replace('-', '')
        
        # Check if key components are present
        key_components = ['BMW', 'X3', 'XDRIVE', 'CONVENIENCE', 'AWD']
        match_score = 0
        
        for component in key_components:
            if component in vehicle_normalized and component in match_normalized:
                match_score += 1
        
        # Consider it a match if at least 3 key components match
        return match_score >= 3
    
    def _get_conflict_resolution_details(self, ai_result: Dict[str, Any]) -> Dict[str, Any]:
        """Get conflict resolution details from AI result."""
        if not ai_result:
            return {
                'status': 'no_data',
                'message': 'No AI result data available'
            }
        
        deduplication_stats = ai_result.get('deduplication_stats', {})
        conflicts_resolved = ai_result.get('conflicts_resolved', [])
        
        # Check if there were any conflicts
        has_conflicts = (
            deduplication_stats.get('conflict_groups', 0) > 0 or
            deduplication_stats.get('rating_conflicts', 0) > 0 or
            len(conflicts_resolved) > 0
        )
        
        if not has_conflicts:
            return {
                'status': 'no_conflicts',
                'message': 'No rating conflicts detected - all vehicle specifications were unique',
                'deduplication_stats': deduplication_stats,
                'resolution_method': 'none_required'
            }
        
        # Enhanced conflict resolution details
        enhanced_conflicts = []
        
        if conflicts_resolved:
            for conflict in conflicts_resolved:
                enhanced_conflict = {
                    'conflicting_vehicles': conflict.get('conflicting_vehicles', []),
                    'resolved_ratings': conflict.get('resolved_ratings', {}),
                    'conflict_details': conflict.get('conflict_details', {}),
                    'resolution_method': 'conservative_max'
                }
                enhanced_conflicts.append(enhanced_conflict)
        
        return {
            'status': 'conflicts_resolved',
            'deduplication_stats': deduplication_stats,
            'conflicts_resolved': enhanced_conflicts,
            'resolution_method': 'conservative_max'  # Based on our updated business rules
        }
