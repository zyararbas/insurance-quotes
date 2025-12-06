import logging
import json
import statistics
import os
import re
from dotenv import load_dotenv
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict

# --- Import official Gemini SDK components ---
try:
    from google import genai
    from google.genai.errors import APIError as GeminiAPIError
except ImportError:
    # This block handles the case where the user forgot to install the SDK
    raise ImportError(
        "The 'google-genai' package is not installed. Please install it using: pip install google-genai"
    )

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@dataclass
class AIProvider:
    """Configuration for AI provider"""
    name: str
    model: str
    api_key_env: str # Included for consistency, but SDK loads it automatically

class AIAssistantServiceGeminiSDK:
    """
    Comprehensive AI Assistant Service using the google-genai SDK for API calls.
    """
    
    # AI Provider configuration - Only Gemini
    PROVIDERS = {
        'gemini': AIProvider(
            name='Gemini SDK',
            model='gemini-flash-lite-latest', # The requested model
            api_key_env='GEMINI_API_KEY'
        )
    }
    
    def __init__(self):
        """
        Initialize the AI Assistant Service and the Gemini client.
        
        The SDK client automatically looks for the GEMINI_API_KEY environment variable.
        """
        load_dotenv() # Load the .env file first
        
        self.provider = self.PROVIDERS['gemini']
        
        # Check if API key is present before initialization
        if not os.getenv(self.provider.api_key_env):
            raise ValueError(
                f"API key not found. Please set {self.provider.api_key_env} in your .env file or environment variables."
            )
            
        # Initialize the Gemini Client
        # The client automatically picks up the API key from the environment.
        self.client = genai.Client()
        
        # Business rules for conflict resolution - use most conservative (highest) rating
        self.conflict_resolution_rules = {
            'GRG': 'max', 'DRG': 'max', 'VSD': 'max', 'LRG': 'max'
        }
        
        # Confidence thresholds for conservative approach
        self.confidence_thresholds = {
            'high': 1, 'medium': 3, 'low': 5, 'exclude': 5
        }
        
    def interpret_vehicle_results(self, 
                                vin_data: Optional[Dict[str, Any]], 
                                search_results: List[Dict[str, Any]], 
                                additional_info: str = "",
                                conversation_history: List[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Use AI assistant (via SDK) to interpret vehicle search results with built-in deduplication.
        """
        try:
            # Step 1: Deduplicate vehicle specifications
            deduplicated_results, conflict_stats = self._deduplicate_vehicle_specs(search_results)
            
            if conflict_stats['conflict_groups'] > 0:
                logging.info(f"Vehicle spec deduplication: {conflict_stats}")
            
            # Step 2: Prepare the context for the AI
            context = self._prepare_context(vin_data, deduplicated_results, additional_info, conversation_history)
            
            # Step 3: Create the prompt
            prompt = self._create_prompt(context)
            
            # Step 4: Call the AI using the SDK
            ai_response = self._call_gemini_sdk(prompt)
            # logging.info(f"=== RAW GEMINI SDK RESPONSE ===")
            # logging.info(f"AI Response: {ai_response}")
            # logging.info(f"=== END RAW GEMINI SDK RESPONSE ===")
            
            # Step 5: Parse the response
            result = self._parse_ai_response(ai_response)
            
            if conflict_stats['conflict_groups'] > 0:
                result['deduplication_stats'] = conflict_stats
            
            return result
            
        except GeminiAPIError as e:
            # Catch specific API errors from the SDK
            logging.error(f"Gemini API Error: {e.status_code} - {e.message}", exc_info=True)
            return {
                'error': f"AI interpretation failed: Gemini API Error ({e.status_code})",
                'questions': ["Could you provide more details about the vehicle?", "What is the body style?", "What is the engine type?"]
            }
        except Exception as e:
            logging.error(f"Error in AI interpretation: {e}", exc_info=True)
            return {
                'error': f"AI interpretation failed: {str(e)}",
                'questions': ["Could you provide more details about the vehicle?", "What is the body style?", "What is the engine type?"]
            }
    
    def _call_gemini_sdk(self, prompt: str) -> str:
        """Call Google Gemini API using the google-genai SDK."""
        
        # System instruction is passed separately for the SDK, providing better context isolation
        system_instruction = "You are an expert insurance analyst specializing in vehicle identification and rating classification."
        
        response = self.client.models.generate_content(
            model=self.provider.model,
            contents=prompt, # The prompt contains the full instruction from _create_prompt
            config=genai.types.GenerateContentConfig(
                system_instruction=system_instruction,
                temperature=0.01,
                top_p=1,
                # max_output_tokens=2000 # Add if needed
            )
        )
        
        # The response object has a 'text' property containing the model's output
        return response.text

    def _deduplicate_vehicle_specs(self, search_results: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Deduplicate vehicle specifications and resolve rating conflicts."""
        if not search_results:
            return [], {'total_vehicles': 0, 'unique_specs': 0, 'conflict_groups': 0}
        
        spec_groups = self._group_by_specification(search_results) 
        deduplicated_results = []
        conflict_log = []
        
        for spec_key, vehicles in spec_groups.items():
            if len(vehicles) == 1:
                deduplicated_results.append(vehicles[0])
            else:
                resolved_vehicle, conflicts = self._resolve_rating_conflicts(vehicles)
                if resolved_vehicle:
                    deduplicated_results.append(resolved_vehicle)
                    if conflicts:
                        conflict_log.extend(conflicts)
                else:
                    logging.warning(f"Excluding vehicle spec {spec_key} due to extreme rating conflicts")
        
        conflict_stats = {
            'total_vehicles': len(search_results), 'unique_specs': len(spec_groups),
            'conflict_groups': len([g for g in spec_groups.values() if len(g) > 1]),
            'conflict_vehicles': sum(len(g) for g in spec_groups.values() if len(g) > 1),
            'rating_conflicts': self._count_rating_conflicts(conflict_log)
        }
        
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
            str(vehicle.get('year', '')), str(vehicle.get('make', '')), str(vehicle.get('model', '')),
            str(vehicle.get('series', '')), str(vehicle.get('package', '')), str(vehicle.get('style', '')),
            str(vehicle.get('engine', '')), str(vehicle.get('wheelbase', ''))
        ]
        return '|'.join(key_components)
    
    # --- Remaining Deduplication and Helper Methods (omitted for brevity, assume they are carried over and correct) ---
    # _resolve_rating_conflicts, _resolve_rating_field_conflict, _calculate_max, 
    # _calculate_median, _calculate_mode, _determine_confidence_level, 
    # _count_rating_conflicts, _log_conflicts, _prepare_context, _create_prompt, _parse_ai_response, get_supported_providers, test_connection

    # --- NOTE: The methods below are essential and included for completeness ---

    def _resolve_rating_conflicts(self, vehicles: List[Dict[str, Any]]) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
        if not vehicles: return None, []
        resolved_vehicle = vehicles[0].copy(); conflicts = []; rating_fields = ['GRG', 'DRG', 'VSD', 'LRG']
        for field in rating_fields:
            values = [v.get(field) for v in vehicles if v.get(field) is not None]
            if len(set(values)) > 1:  # Conflict detected
                conflict_info = self._resolve_rating_field_conflict(field, values, vehicles)
                resolved_vehicle[field] = conflict_info['resolved_value']
                conflicts.append(conflict_info)
        return resolved_vehicle, conflicts
    
    def _resolve_rating_field_conflict(self, field: str, values: List[Any], vehicles: List[Dict[str, Any]]) -> Dict[str, Any]:
        rule = self.conflict_resolution_rules.get(field, 'median')
        if field == 'VSD': max_diff = len(set(values)) - 1
        else:
            numeric_values = [float(v) for v in values if v is not None]
            max_diff = max(numeric_values) - min(numeric_values) if numeric_values else 0
        confidence = self._determine_confidence_level(max_diff)
        if rule == 'max': resolved_value = self._calculate_max(values)
        elif rule == 'median': resolved_value = self._calculate_median(values)
        elif rule == 'mode': resolved_value = self._calculate_mode(values)
        else: resolved_value = values[0]
        return {'field': field, 'values': values, 'resolved_value': resolved_value, 'max_difference': max_diff, 'confidence': confidence, 'rule': rule, 'vehicle_count': len(vehicles)}
    
    def _calculate_max(self, values: List[Any]) -> Any:
        try: numeric_values = [float(v) for v in values if v is not None]
        except (ValueError, TypeError): return max(values) if values else None
        return max(numeric_values) if numeric_values else (max(values) if values else None)
    
    def _calculate_median(self, values: List[Any]) -> Any:
        try: numeric_values = [float(v) for v in values if v is not None]
        except (ValueError, TypeError): return values[0] if values else None
        return statistics.median(numeric_values) if numeric_values else (values[0] if values else None)
    
    def _calculate_mode(self, values: List[Any]) -> Any:
        if not values: return None
        value_counts = defaultdict(int);
        for value in values: value_counts[value] += 1
        return max(value_counts, key=value_counts.get)
    
    def _determine_confidence_level(self, max_difference: float) -> str:
        if max_difference <= self.confidence_thresholds['high']: return 'high'
        elif max_difference <= self.confidence_thresholds['medium']: return 'medium'
        elif max_difference <= self.confidence_thresholds['low']: return 'low'
        else: return 'exclude'
    
    def _count_rating_conflicts(self, conflict_log: List[Dict[str, Any]]) -> Dict[str, int]:
        rating_conflicts = defaultdict(int);
        for conflict in conflict_log: rating_conflicts[conflict['field']] += 1
        return dict(rating_conflicts)
    
    def _log_conflicts(self, conflicts: List[Dict[str, Any]]):
        for conflict in conflicts:
            logging.warning(f"Rating conflict resolved: {conflict['field']} = {conflict['resolved_value']} (from {conflict['values']}, diff={conflict['max_difference']}, confidence={conflict['confidence']}, rule={conflict['rule']})")
    
    def _prepare_context(self, vin_data: Optional[Dict], search_results: List[Dict], additional_info: str, conversation_history: List[Dict[str, str]] = None) -> Dict[str, Any]:
        context = {'search_results': search_results, 'additional_info': additional_info, 'total_matches': len(search_results), 'conversation_history': conversation_history or []}
        if vin_data:
            context['vin_data'] = {k: vin_data.get(k) for k in ['make', 'model', 'year', 'body_class', 'engine_config', 'fuel_type', 'drive_type', 'transmission', 'trim', 'series']}
        return context
    
    def _create_prompt(self, context: Dict[str, Any]) -> str:
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
- When preparing question, the id must match the field name. Double check, let's think step by step.
"""
        if context.get('vin_data'):
            vin_data = context['vin_data']; prompt += f"\n**SEARCH CRITERIA:**\n"
            prompt += f"Year: {vin_data.get('year', '')}\n"; prompt += f"Make: {vin_data.get('make', '')}\n"; prompt += f"Model: {vin_data.get('model', '')}\n"
            if vin_data.get('body_class'): prompt += f"Body Class: {vin_data.get('body_class')}\n"
            if vin_data.get('drive_type'): prompt += f"Drive Type: {vin_data.get('drive_type')}\n"
            if vin_data.get('engine_config'): prompt += f"Engine: {vin_data.get('engine_config')}\n"
        elif context['search_results']:
            first_vehicle = context['search_results'][0]; prompt += f"\n**SEARCH CRITERIA:**\n"
            prompt += f"Year: {first_vehicle.get('year', '')}\n"; prompt += f"Make: {first_vehicle.get('make', '')}\n"; prompt += f"Model: {first_vehicle.get('model', '')}\n"
        prompt += f"\n**ADDITIONAL INFORMATION:**\n{context['additional_info'] or '(No additional information provided)'}\n"
        prompt += f"\nPossible Matches ({context['total_matches']} vehicles):\n"; prompt += "Year Make Model Series Package Style Engine\n"
        for vehicle in context['search_results']:
            year = vehicle.get('year', ''); make = vehicle.get('make', ''); model = vehicle.get('model', ''); series = vehicle.get('series', '') or '-'
            package = vehicle.get('package', '') or '-'; style = vehicle.get('style', '') or '-'; engine = vehicle.get('engine', '') or '-'
            prompt += f"{year} {make} {model} {series} {package} {style} {engine}\n"
        if context.get('conversation_history'):
            prompt += f"\nPrevious Conversation:\n"
            for i, exchange in enumerate(context['conversation_history'], 1):
                prompt += f"Q{i}: {exchange.get('question', '')}\n"; prompt += f"A{i}: {exchange.get('answer', '')}\n\n"
            prompt += f"\nIMPORTANT: Based on the conversation above, you now have enough information to make a match. Use the answers provided to find the exact vehicle from the list above. Return the vehicle as a match string in the format: 'YEAR MAKE MODEL [SERIES] [PACKAGE] (STYLE)'\n"
            prompt += f"Use parentheses around the style: (2D CPE), (2D CV), etc. Do NOT include engine or wheelbase.\n"
        return prompt
    
    def _parse_ai_response(self, ai_response: str) -> Dict[str, Any]:
        """Parse the AI response and extract structured data."""
        try:
            json_match = re.search(r'\{.*\}', ai_response, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
            else:
                logging.warning("No JSON object found in AI response. Returning default questions.")
                return {'questions': ['Could you provide more details about the vehicle?']}
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse AI response as JSON: {e}")
            return {'questions': ['Could you provide more details about the vehicle?']}
    
    def get_supported_providers(self) -> List[str]:
        """Get list of supported AI providers (only Gemini)."""
        return list(self.PROVIDERS.keys())
    
    def test_connection(self) -> Dict[str, Any]:
        """Test the AI API connection."""
        try:
            test_prompt = "Hello, this is a test. Please respond with a simple JSON object: {\"status\": \"successful\"}"
            response = self._call_gemini_sdk(test_prompt)
            parsed_response = self._parse_ai_response(response)
            return {
                'status': 'success',
                'provider': self.provider.name,
                'model': self.provider.model,
                'response_snippet': response[:100] + '...' if len(response) > 100 else response,
                'parsed_status': parsed_response.get('status')
            }
        except Exception as e:
            return {
                'status': 'error',
                'provider': self.provider.name,
                'error': str(e)
            }