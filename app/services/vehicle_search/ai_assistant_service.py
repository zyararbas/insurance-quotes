"""
AI Assistant Service with Built-in Deduplication

This service combines AI-powered vehicle matching with automatic deduplication
and conflict resolution for vehicle specifications.
"""

import requests
import logging
import json
import statistics
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@dataclass
class AIProvider:
    """Configuration for AI provider"""
    name: str
    api_url: str
    api_key_env: str
    model: str

class AIAssistantService:
    """
    Comprehensive AI Assistant Service with built-in vehicle specification deduplication.
    
    This service combines AI-powered vehicle matching with automatic deduplication
    and conflict resolution for vehicle specifications.
    """
    
    # AI Provider configurations
    PROVIDERS = {
        'openai': AIProvider(
            name='OpenAI',
            api_url='https://api.openai.com/v1/chat/completions',
            api_key_env='OPENAI_API_KEY',
            model='gpt-4.1-mini'
        )
    }
    
    def __init__(self, provider: str = 'openai'):
        """
        Initialize the AI Assistant Service.
        
        Args:
            provider (str): AI provider to use ('openai')
        """
        if provider not in self.PROVIDERS:
            raise ValueError(f"Unsupported provider: {provider}. Supported providers: {list(self.PROVIDERS.keys())}")
        
        self.provider = self.PROVIDERS[provider]
        self.api_key = self._get_api_key()
        
        # Business rules for conflict resolution - use most conservative (highest) rating
        self.conflict_resolution_rules = {
            'GRG': 'max',         # Garage Rating Group - use highest (most conservative)
            'DRG': 'max',         # Driver Rating Group - use highest (most conservative)
            'VSD': 'max',         # Vehicle Safety Data - use highest (most conservative)
            'LRG': 'max'          # Loss Rating Group - use highest (most conservative)
        }
        
        # Confidence thresholds for conservative approach
        self.confidence_thresholds = {
            'high': 1,      # Difference <= 1: High confidence (very close values)
            'medium': 3,    # Difference 2-3: Medium confidence
            'low': 5,       # Difference 4-5: Low confidence
            'exclude': 5    # Difference > 5: Exclude from AI model (too much variation)
        }
        
    def _get_api_key(self) -> str:
        """Get API key from environment variable or .env file."""
        import os
        from dotenv import load_dotenv
        
        # Load .env file if it exists
        load_dotenv()
        
        api_key = os.getenv(self.provider.api_key_env)
        if not api_key:
            raise ValueError(f"API key not found. Please set {self.provider.api_key_env} in your .env file or environment variables.")
        return api_key
    
    def interpret_vehicle_results(self, 
                                vin_data: Optional[Dict[str, Any]], 
                                search_results: List[Dict[str, Any]], 
                                additional_info: str = "",
                                conversation_history: List[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Use AI assistant to interpret vehicle search results with built-in deduplication.
        
        This method automatically deduplicates vehicle specifications and resolves
        rating conflicts before sending to the AI model.
        
        Args:
            vin_data (Optional[Dict]): VIN lookup data from NHTSA API
            search_results (List[Dict]): List of matching vehicles from database
            additional_info (str): Additional unstructured information about the vehicle
            conversation_history (List[Dict]): Previous Q&A conversation history
            
        Returns:
            Dict: AI interpretation result with either exact match or follow-up questions
        """
        try:
            # Step 1: Deduplicate vehicle specifications
            deduplicated_results, conflict_stats = self._deduplicate_vehicle_specs(search_results)
            
            # Log deduplication statistics
            if conflict_stats['conflict_groups'] > 0:
                logging.info(f"Vehicle spec deduplication: {conflict_stats}")
            
            # Step 2: Prepare the context for the AI
            context = self._prepare_context(vin_data, deduplicated_results, additional_info, conversation_history)
            
            # Step 3: Create the prompt
            prompt = self._create_prompt(context)
            
            # Step 4: Call the AI
            ai_response = self._call_ai_api(prompt)
            logging.info(f"=== RAW AI RESPONSE ===")
            logging.info(f"AI Response: {ai_response}")
            logging.info(f"=== END RAW AI RESPONSE ===")
            
            # Step 5: Parse the response
            result = self._parse_ai_response(ai_response)
            logging.info(f"=== PARSED AI RESULT ===")
            logging.info(f"Parsed Result: {result}")
            logging.info(f"=== END PARSED AI RESULT ===")
            
            # Add deduplication info to result
            if conflict_stats['conflict_groups'] > 0:
                result['deduplication_stats'] = conflict_stats
            
            return result
            
        except Exception as e:
            logging.error(f"Error in AI interpretation: {e}", exc_info=True)
            return {
                'error': f"AI interpretation failed: {str(e)}",
                'questions': [
                    "Could you provide more details about the vehicle?",
                    "What is the body style (2D, 4D, SUV, etc.)?",
                    "What is the engine type or displacement?"
                ]
            }
    
    def _deduplicate_vehicle_specs(self, search_results: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Deduplicate vehicle specifications and resolve rating conflicts.
        
        Args:
            search_results: List of vehicle search results with potential duplicates
            
        Returns:
            Tuple of (deduplicated_results, conflict_statistics)
        """
        if not search_results:
            return [], {'total_vehicles': 0, 'unique_specs': 0, 'conflict_groups': 0}
        
        # Group vehicles by specification
        spec_groups = self._group_by_specification(search_results)
        
        # Resolve conflicts within each group
        deduplicated_results = []
        conflict_log = []
        
        for spec_key, vehicles in spec_groups.items():
            if len(vehicles) == 1:
                # No conflicts, use as-is
                deduplicated_results.append(vehicles[0])
            else:
                # Resolve conflicts
                resolved_vehicle, conflicts = self._resolve_rating_conflicts(vehicles)
                if resolved_vehicle:
                    deduplicated_results.append(resolved_vehicle)
                    if conflicts:
                        conflict_log.extend(conflicts)
                else:
                    # Exclude from AI model due to extreme conflicts
                    logging.warning(f"Excluding vehicle spec {spec_key} due to extreme rating conflicts")
        
        # Calculate conflict statistics
        conflict_stats = {
            'total_vehicles': len(search_results),
            'unique_specs': len(spec_groups),
            'conflict_groups': len([g for g in spec_groups.values() if len(g) > 1]),
            'conflict_vehicles': sum(len(g) for g in spec_groups.values() if len(g) > 1),
            'rating_conflicts': self._count_rating_conflicts(conflict_log)
        }
        
        # Log conflicts for monitoring
        if conflict_log:
            self._log_conflicts(conflict_log)
        
        return deduplicated_results, conflict_stats
    
    def _group_by_specification(self, vehicles: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Group vehicles by their specification (excluding ratings)."""
        spec_groups = defaultdict(list)
        
        for vehicle in vehicles:
            spec_key = self._create_specification_key(vehicle)
            spec_groups[spec_key].append(vehicle)
        
        return dict(spec_groups)
    
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
    
    def _resolve_rating_conflicts(self, vehicles: List[Dict[str, Any]]) -> tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
        """Resolve rating conflicts for vehicles with identical specifications."""
        if not vehicles:
            return None, []
        
        # Use first vehicle as base
        resolved_vehicle = vehicles[0].copy()
        conflicts = []
        
        # Check each rating field for conflicts
        rating_fields = ['GRG', 'DRG', 'VSD', 'LRG']
        
        for field in rating_fields:
            values = [v.get(field) for v in vehicles if v.get(field) is not None]
            
            if len(set(values)) > 1:  # Conflict detected
                conflict_info = self._resolve_rating_field_conflict(field, values, vehicles)
                resolved_vehicle[field] = conflict_info['resolved_value']
                conflicts.append(conflict_info)
        
        return resolved_vehicle, conflicts
    
    def _resolve_rating_field_conflict(self, field: str, values: List[Any], vehicles: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Resolve conflict for a specific rating field."""
        rule = self.conflict_resolution_rules.get(field, 'median')
        
        # Calculate difference magnitude
        if field == 'VSD':
            max_diff = len(set(values)) - 1
        else:
            numeric_values = [float(v) for v in values if v is not None]
            if numeric_values:
                max_diff = max(numeric_values) - min(numeric_values)
            else:
                max_diff = 0
        
        # Determine confidence level
        confidence = self._determine_confidence_level(max_diff)
        
        # Apply resolution rule
        if rule == 'max':
            resolved_value = self._calculate_max(values)
        elif rule == 'median':
            resolved_value = self._calculate_median(values)
        elif rule == 'mode':
            resolved_value = self._calculate_mode(values)
        else:
            resolved_value = values[0]
        
        return {
            'field': field,
            'values': values,
            'resolved_value': resolved_value,
            'max_difference': max_diff,
            'confidence': confidence,
            'rule': rule,
            'vehicle_count': len(vehicles)
        }
    
    def _calculate_max(self, values: List[Any]) -> Any:
        """Calculate maximum (most conservative) value for a list of values."""
        try:
            # For numeric values, use the maximum
            numeric_values = [float(v) for v in values if v is not None]
            if numeric_values:
                return max(numeric_values)
            else:
                # For non-numeric values (like VSD), use the highest alphabetical value
                # This ensures consistent ordering for categorical data
                return max(values) if values else None
        except (ValueError, TypeError):
            # If conversion fails, use the highest value as-is
            return max(values) if values else None
    
    def _calculate_median(self, values: List[Any]) -> Any:
        """Calculate median value for a list of values."""
        try:
            numeric_values = [float(v) for v in values if v is not None]
            if numeric_values:
                return statistics.median(numeric_values)
            else:
                return values[0] if values else None
        except (ValueError, TypeError):
            return values[0] if values else None
    
    def _calculate_mode(self, values: List[Any]) -> Any:
        """Calculate mode (most common value) for a list of values."""
        if not values:
            return None
        
        value_counts = defaultdict(int)
        for value in values:
            value_counts[value] += 1
        
        return max(value_counts, key=value_counts.get)
    
    def _determine_confidence_level(self, max_difference: float) -> str:
        """Determine confidence level based on difference magnitude."""
        if max_difference <= self.confidence_thresholds['high']:
            return 'high'
        elif max_difference <= self.confidence_thresholds['medium']:
            return 'medium'
        elif max_difference <= self.confidence_thresholds['low']:
            return 'low'
        else:
            return 'exclude'
    
    def _count_rating_conflicts(self, conflict_log: List[Dict[str, Any]]) -> Dict[str, int]:
        """Count conflicts by rating field."""
        rating_conflicts = defaultdict(int)
        for conflict in conflict_log:
            rating_conflicts[conflict['field']] += 1
        return dict(rating_conflicts)
    
    def _log_conflicts(self, conflicts: List[Dict[str, Any]]):
        """Log rating conflicts for analysis and monitoring."""
        for conflict in conflicts:
            logging.warning(
                f"Rating conflict resolved: {conflict['field']} = {conflict['resolved_value']} "
                f"(from {conflict['values']}, diff={conflict['max_difference']}, "
                f"confidence={conflict['confidence']}, rule={conflict['rule']})"
            )
    
    def _prepare_context(self, vin_data: Optional[Dict], search_results: List[Dict], additional_info: str, conversation_history: List[Dict[str, str]] = None) -> Dict[str, Any]:
        """Prepare context data for AI analysis."""
        context = {
            'search_results': search_results,
            'additional_info': additional_info,
            'total_matches': len(search_results),
            'conversation_history': conversation_history or []
        }
        
        if vin_data:
            context['vin_data'] = {
                'make': vin_data.get('make'),
                'model': vin_data.get('model'),
                'year': vin_data.get('year'),
                'body_class': vin_data.get('body_class'),
                'engine_config': vin_data.get('engine_config'),
                'fuel_type': vin_data.get('fuel_type'),
                'drive_type': vin_data.get('drive_type'),
                'transmission': vin_data.get('transmission'),
                'trim': vin_data.get('trim'),
                'series': vin_data.get('series')
            }
        
        return context
    
    def _create_prompt(self, context: Dict[str, Any]) -> str:
        """Create the prompt for the AI assistant."""
        prompt = """You are an insurance analyst, who's task is to match car information to a specific Vehicle spec in a table of insurance vehicle ratings, in order to return the vehicle ratings for pricing. 
I will give you the Matching results based on an exact match of Year, Make and Model from the Table, as well as unstructured additional information that may be relevant to narrowing down the search (AWD, 2D, Etc...) 
Your task is to: 
Attempt to find a single vehicle spec from the provided list that matches the unstructured car data provided
If no, single match can be found, return a series of questions that we can present the user that will narrow down the selection to a single match within the provided list (i.e. 4D or 2D?)

Please respond with a JSON object in the following format:
{
    "match": "YEAR MAKE MODEL [SERIES] [PACKAGE] (STYLE)" (if confirmed match found),
    "questions": [
        {
            "question": "What is the body style?",
            "options": ["2D Coupe", "4D Sedan", "SUV", "Hatchback"],
            "id": "body_style"
        }
    ] (only if no confirmed match)
}

IMPORTANT: When returning a match, use this EXACT format:
- Include SERIES only if it exists and is not empty
- Include PACKAGE only if it exists and is not empty  
- Always use parentheses around STYLE: (2D CPE), (2D CV), etc.
- Do NOT include engine or wheelbase information
- Example: "2022 BENTLEY CONTINENTAL GT V8 (2D CPE)"

"""
        
        # Add search criteria explicitly
        if context.get('vin_data'):
            # Extract search criteria from VIN data
            year = context['vin_data'].get('year', '')
            make = context['vin_data'].get('make', '')
            model = context['vin_data'].get('model', '')
            prompt += f"\n**SEARCH CRITERIA:**\n"
            prompt += f"Year: {year}\n"
            prompt += f"Make: {make}\n"
            prompt += f"Model: {model}\n"
            if context['vin_data'].get('body_class'):
                prompt += f"Body Class: {context['vin_data'].get('body_class')}\n"
            if context['vin_data'].get('drive_type'):
                prompt += f"Drive Type: {context['vin_data'].get('drive_type')}\n"
            if context['vin_data'].get('engine_config'):
                prompt += f"Engine: {context['vin_data'].get('engine_config')}\n"
        else:
            # If no VIN data, we need to extract from search results
            if context['search_results']:
                first_vehicle = context['search_results'][0]
                year = first_vehicle.get('year', '')
                make = first_vehicle.get('make', '')
                model = first_vehicle.get('model', '')
                prompt += f"\n**SEARCH CRITERIA:**\n"
                prompt += f"Year: {year}\n"
                prompt += f"Make: {make}\n"
                prompt += f"Model: {model}\n"
        
        # Add additional info
        if context['additional_info']:
            prompt += f"\n**ADDITIONAL INFORMATION:**\n{context['additional_info']}\n"
            logging.info(f"Additional info sent to AI: {context['additional_info']}")
        else:
            prompt += f"\n**ADDITIONAL INFORMATION:**\n(No additional information provided)\n"
            logging.info("No additional info provided to AI")
        
        # Add search results as table format
        prompt += f"\nPossible Matches ({context['total_matches']} vehicles):\n"
        prompt += "Year Make Model Series Package Style Engine\n"
        for vehicle in context['search_results']:
            # Create table row format
            year = vehicle.get('year', '')
            make = vehicle.get('make', '')
            model = vehicle.get('model', '')
            series = vehicle.get('series', '') or '-'
            package = vehicle.get('package', '') or '-'
            style = vehicle.get('style', '') or '-'
            engine = vehicle.get('engine', '') or '-'
            
            prompt += f"{year} {make} {model} {series} {package} {style} {engine}\n"
            
            # Log vehicles with packages for debugging
            if vehicle.get('package'):
                logging.info(f"Vehicle has package: {vehicle.get('package')}")
        
        # Add conversation history if available
        if context.get('conversation_history'):
            prompt += f"\nPrevious Conversation:\n"
            for i, exchange in enumerate(context['conversation_history'], 1):
                prompt += f"Q{i}: {exchange.get('question', '')}\n"
                prompt += f"A{i}: {exchange.get('answer', '')}\n\n"
            
            # Add explicit instruction to return a match after questions are answered
            prompt += f"\nIMPORTANT: Based on the conversation above, you now have enough information to make a match. "
            prompt += f"Use the answers provided to find the exact vehicle from the list above. "
            prompt += f"Return the vehicle as a match string in the format: 'YEAR MAKE MODEL [SERIES] [PACKAGE] (STYLE)'\n"
            prompt += f"Use parentheses around the style: (2D CPE), (2D CV), etc. Do NOT include engine or wheelbase.\n"
        
        return prompt
    
    def _call_ai_api(self, prompt: str) -> str:
        """Call the AI API based on the configured provider."""
        if self.provider.name == 'OpenAI':
            return self._call_openai_api(prompt)
        else:
            raise ValueError(f"Unsupported provider: {self.provider.name}")
    
    def _call_openai_api(self, prompt: str) -> str:
        """Call OpenAI API."""
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
        
        data = {
            'model': self.provider.model,
            'messages': [
                {
                    'role': 'system',
                    'content': 'You are an expert insurance analyst specializing in vehicle identification and rating classification.'
                },
                {
                    'role': 'user',
                    'content': prompt
                }
            ],
            'temperature': 0.01,
            'top_p': 1,
            'max_tokens': 2000
        }
        
        response = requests.post(self.provider.api_url, headers=headers, json=data, timeout=30)
        response.raise_for_status()
        
        result = response.json()
        return result['choices'][0]['message']['content']
    
    def _parse_ai_response(self, ai_response: str) -> Dict[str, Any]:
        """Parse the AI response and extract structured data."""
        try:
            # Try to extract JSON from the response
            import re
            json_match = re.search(r'\{.*\}', ai_response, re.DOTALL)
            if json_match:
                json_str = json_match.group()
                return json.loads(json_str)
            else:
                # Fallback if no JSON found
                return {
                    'questions': ['Could you provide more details about the vehicle?']
                }
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse AI response as JSON: {e}")
            return {
                'questions': ['Could you provide more details about the vehicle?']
            }
    
    def get_supported_providers(self) -> List[str]:
        """Get list of supported AI providers."""
        return list(self.PROVIDERS.keys())
    
    def test_connection(self) -> Dict[str, Any]:
        """Test the AI API connection."""
        try:
            test_prompt = "Hello, this is a test. Please respond with 'Connection successful'."
            response = self._call_ai_api(test_prompt)
            return {
                'status': 'success',
                'provider': self.provider.name,
                'response': response[:100] + '...' if len(response) > 100 else response
            }
        except Exception as e:
            return {
                'status': 'error',
                'provider': self.provider.name,
                'error': str(e)
            }
