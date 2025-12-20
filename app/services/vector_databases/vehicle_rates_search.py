import os
import pandas as pd
import chromadb
import shutil
import time
import ast
import re
from chromadb.utils import embedding_functions
from typing import Optional
from abc import ABC, abstractmethod 

# Define "junk" words that don't help identify a model
MODEL_STOP_WORDS = {
    'class', 'series', 'sedan', 'suv', 'coupe', 'ev', 'hybrid', 
    'benz', 'na', 'n', 'a' # 'na', 'n', 'a' handle 'N/A' after tokenizing
}

# -------------------------------------------------
# 1. ABSTRACT LOADER INTERFACE
# -------------------------------------------------
class IDataLoader(ABC):
    """
    An interface for all data loaders. Any loader (CSV, Mongo, etc.)
    must implement the index_data method.
    """
    @abstractmethod
    def index_data(self, collection):
        """
        This method should read from a data source (file, db, api)
        and add the data to the provided ChromaDB collection.
        It must save metadata with lowercase keys:
        'year', 'make', 'model', 'series', 'package', 'style', 'engine', 
        'grg', 'drg', 'vsd', 'lrg', 'expiration'
        """
        pass

# -------------------------------------------------
# 2. THE MAIN VEHICLE VECTOR DB CLASS
# -------------------------------------------------
class VehicleRatesVectorDB:
    def __init__(self, db_folder="./vehicle_rates_rag"):
        """
        Initializes the Local Vector Database.
        It now accepts a 'data_loader' to handle indexing.
        """
        print(f"--- Initializing ChromaDB (v{chromadb.__version__}) ---")  
        # 2. Connect to local persistent storage
        self.client = chromadb.PersistentClient(path=db_folder)
        
        # 3. Define the embedding model
        self.emb_fn = embedding_functions.SentenceTransformerEmbeddingFunction(
            model_name="all-MiniLM-L6-v2"
        )
        
        # 4. Get or Create the Collection
        self.collection = self.client.get_or_create_collection(
            name="vehicle_ratings_v1",
            embedding_function=self.emb_fn
        )
    
    def _tokenize_model(self, model_name):
        """
        Helper to robustly tokenize model/trim names and remove stop words.
        Splits letters from numbers (e.g., 'EQB300' -> 'eqb 300').
        """
        s = str(model_name) # Handle potential int/digits
        # Insert space between letters and numbers
        s = re.sub(r'([a-zA-Z])([0-9])', r'\1 \2', s)
        s = re.sub(r'([0-9])([a-zA-Z])', r'\1 \2', s)
        
        # Replace hyphens/slashes with spaces, strip non-alphanumeric, lowercase
        tokens = re.sub(r'[^a-z0-9\s]+', '', s.lower().replace('-', ' '))
        
        # Split by space, filter out empty tokens and stop words
        return set(filter(lambda t: t and t not in MODEL_STOP_WORDS, tokens.split()))

    def _jaccard_similarity(self, set_a, set_b):
        """Calculates Jaccard similarity between two sets."""
        if not set_a and not set_b:
            return 0.0
        intersection = set_a.intersection(set_b)
        union = set_a.union(set_b)
        if not union:
             return 0.0
        return len(intersection) / len(union)

    def search_with_boosting(self, query_text, boost_targets, weights, top_k=5, where=None, strict_model_match=None):
        """
        Performs Vector Search + Jaccard Similarity Boosting (Re-ranking).
        Boosts: Make, Model, Year, Trim (vs series/package), Style, Engine
        Supports strict filtering via 'where' (Chroma) and 'strict_model_match' (Post-filter).
        """
        results = self.collection.query(
            query_texts=[query_text],
            n_results=50, # Fetch wide
            where=where
        )
        
        reranked_candidates = []
        
        if results['ids'] and len(results['ids']) > 0:
            # Pre-tokenize targets
            target_make_tokens = self._tokenize_model(boost_targets.get('make', ''))
            target_model_tokens = self._tokenize_model(boost_targets.get('model', ''))
            target_trim_tokens = self._tokenize_model(boost_targets.get('trim', ''))
            target_style_tokens = self._tokenize_model(boost_targets.get('style', ''))
            target_engine_tokens = self._tokenize_model(boost_targets.get('engine', ''))
            
            for i in range(len(results['ids'][0])):
                meta = results['metadatas'][0][i]
                original_dist = results['distances'][0][i]
                new_score = original_dist
                
                # A. Make Boost (Proportional Jaccard Similarity)
                candidate_make_tokens = self._tokenize_model(meta.get('make', ''))
                jaccard_score_make = self._jaccard_similarity(target_make_tokens, candidate_make_tokens)
                new_score -= (weights.get('make', 0.0) * jaccard_score_make)
                    
                # B. Model Boost (Proportional Jaccard Similarity)
                candidate_model_tokens = self._tokenize_model(meta.get('model', ''))
                jaccard_score_model = self._jaccard_similarity(target_model_tokens, candidate_model_tokens)
                new_score -= (weights.get('model', 0.0) * jaccard_score_model)

                # C. Year Boost (Numerical)
                target_year = int(boost_targets.get('year', 0))
                candidate_year = int(meta.get('year', 0))
                if target_year > 0:
                    if candidate_year == target_year:
                        new_score -= weights.get('year', 0.0)
                    elif abs(candidate_year - target_year) <= 2:
                        new_score -= (weights.get('year', 0.0) / 2)

                # D. Flexible Trim Boost (Proportional Jaccard Similarity)
                # Checks trim against series and package
                candidate_series_tokens = self._tokenize_model(meta.get('series', ''))
                candidate_package_tokens = self._tokenize_model(meta.get('package', ''))
                
                score_series = self._jaccard_similarity(target_trim_tokens, candidate_series_tokens)
                score_package = self._jaccard_similarity(target_trim_tokens, candidate_package_tokens)
                
                best_trim_score = max(score_series, score_package)
                new_score -= (weights.get('trim', 0.0) * best_trim_score)

                # E. Style Boost (Proportional Jaccard Similarity)
                candidate_style_tokens = self._tokenize_model(meta.get('style', ''))
                jaccard_score_style = self._jaccard_similarity(target_style_tokens, candidate_style_tokens)
                new_score -= (weights.get('style', 0.0) * jaccard_score_style)
                
                # F. Engine Boost (Proportional Jaccard Similarity)
                candidate_engine_tokens = self._tokenize_model(meta.get('engine', ''))
                jaccard_score_engine = self._jaccard_similarity(target_engine_tokens, candidate_engine_tokens)
                new_score -= (weights.get('engine', 0.0) * jaccard_score_engine)

                # STRICT MODEL FILTERING
                # If strict_model_match is set, only keep candidates where the model name matches significantly
                keep_candidate = True
                if strict_model_match:
                    target_model_tokens_strict = self._tokenize_model(strict_model_match)
                    # Require that ALL tokens in the target model are present in the candidate
                    # This prevents "R1S" matching "R1T" (where 's' is missing from 'R1T')
                    if not target_model_tokens_strict.issubset(candidate_model_tokens):
                         keep_candidate = False
                
                if keep_candidate:
                    canddidate = {
                        "Vehicle Info": results['documents'][0][i],
                        "year": boost_targets.get('year', meta.get('year')),
                        "make": meta.get('make'),
                        "model": meta.get('model'),
                        "grg": meta.get('grg', 'N/A'),
                        "drg": meta.get('drg', 'N/A'),
                        "vsd": meta.get('vsd', 'N/A'),
                        "lrg": meta.get('lrg', 'N/A'),
                        "Match Distance": round(new_score, 4)
                    }
                    if meta.get('series'):
                        canddidate['series'] = meta.get('series')
                    if meta.get('package'):
                        canddidate['package'] = meta.get('package')
                    if meta.get('style'):
                        canddidate['style'] = meta.get('style')
                    if meta.get('engine'):
                        canddidate['engine'] = meta.get('engine')
                    reranked_candidates.append(canddidate) 
        reranked_candidates.sort(key=lambda x: x['Match Distance'])
        return reranked_candidates[:top_k]

    def search_by_text(self, query_text, top_k=5, boost_weights=None):
        """
        Standard semantic search. 
        Detects if the query string contains boosting instructions and a JSON dict.
        """
        if "should be boosted" in query_text:
            try:
                match = re.search(r"\{.*\}", query_text)
                if match:
                    dict_str = match.group(0)
                    vehicle_dict = ast.literal_eval(dict_str)
                    
                    boost_targets = {}
                    if 'modelYear' in vehicle_dict: boost_targets['year'] = int(vehicle_dict['modelYear'])
                    if 'year' in vehicle_dict: boost_targets['year'] = int(vehicle_dict['year'])
                    if 'make' in vehicle_dict: boost_targets['make'] = vehicle_dict['make']
                    if 'model' in vehicle_dict: boost_targets['model'] = vehicle_dict['model']
                    if 'engine' in vehicle_dict: boost_targets['engine'] = vehicle_dict['engine']
                    
                    # Map trim/bodyType to 'trim' and 'style' for boosting
                    if 'trim' in vehicle_dict: 
                        boost_targets['trim'] = vehicle_dict['trim']
                    elif 'bodyType' in vehicle_dict: # Fallback for trim
                        boost_targets['trim'] = vehicle_dict['bodyType']
                    
                    if 'style' in vehicle_dict:
                        boost_targets['style'] = vehicle_dict['style']
                    elif 'bodyType' in vehicle_dict: # Fallback for style
                        boost_targets['style'] = vehicle_dict['bodyType']

                    
                    weights = boost_weights if boost_weights is not None else {
                        'make': 0.5, 'model': 0.3, 'year': 0.1, 'trim': 0.2, 
                        'style': 0.1, 'engine': 0.1 # Low default boost
                    }
                    
                    print(f"\n[Boosting] Detected targets: {boost_targets} with weights: {weights}")
                    return self.search_with_boosting(query_text, boost_targets, weights, top_k)
            except Exception as e:
                print(f"[Warning] Boosting parsing failed: {e}. Proceeding with standard search.")

        results = self.collection.query(
            query_texts=[query_text],
            n_results=top_k
        )
        return self._format_results(results)

    def search_by_vin_data(self, vin_data, boosting=True, boost_weights=None):
        """
        Searches the DB based on a VIN data dictionary.
        """
        year_str = str(vin_data.get('year', ''))
        make = vin_data.get('make', '')
        model = vin_data.get('model', '')
        style = vin_data.get('body_class', vin_data.get('style', '')) 
        trim = vin_data.get('trim', '')
        engine = vin_data.get('engine', '') # Get engine
        doors = vin_data.get('doors', '') # Get engine

        query = f"""
                Find me exact or similar vehicles like this year {year_str} make {make} model {model} style {style} trim {trim} engine {engine} doors {doors},  
                make sure both make and model match ,
                if there is a different make or model, if both do not match your critierie then ignore
        """
        
        if boosting:
            boost_targets = {
                'year': int(year_str) if year_str.isdigit() else 0,
                'make': make,
                'model': model,
                'trim': trim,
                'style': style,
                'engine': engine,
                'doors': doors
            }
            weights = boost_weights if boost_weights is not None else {'make': 1.0, 'model': 2.0, 'year': 1.0, 'trim': 1.5, 'style': 0.1, 'engine': 0.5} 

            print(f"\nUsing Search Query: '{query}' (with boosting: {weights})")
            
            # Use strict filtering for Make and Model
            where_clause = {"make": make} if make else None
            
            return self.search_with_boosting(
                query, 
                boost_targets, 
                weights, 
                where=where_clause,
                strict_model_match=model
            )
        else:
            print(f"\nUsing Search Query: '{query}' (boosting disabled)")
            results = self.collection.query(
                query_texts=[query],
                n_results=5
            )
            return self._format_results(results)

    def _format_results(self, results):
        """
        Converts raw Chroma query results (a dict) or a boosted DataFrame
        to a clean, consistent DataFrame.
        """
        # Define all columns we want to see
        all_cols = [
            "Vehicle Info", "Year", "Make", "Model", "series", "package", 
            "style", "engine", "Match Distance","drg","grg","vsd","lrg"
        ]
        
        # Handle if results are already a DataFrame (from boosting)
        # if isinstance(results, pd.DataFrame):
        #     for col in all_cols:
        #         if col not in results.columns:
        #             results[col] = "N/A" # Add missing columns
        #     return results[all_cols]
            
        # Handle raw Chroma dict results
        hits = []
        if results['ids'] and len(results['ids']) > 0:
            for i in range(len(results['ids'][0])):
                meta = results['metadatas'][0][i]
                hits.append({ 
                    "year": meta.get('year'),
                    "make": meta.get('make'),
                    "model": meta.get('model'),
                    "series": meta.get('series', 'N/A'),
                    "package": meta.get('package', 'N/A'),
                    "style": meta.get('style', 'N/A'), 
                    "engine": meta.get('engine', 'N/A'), 
                    "grg": meta.get('grg', 'N/A'), 
                    "drg": meta.get('drg', 'N/A'),
                    "vsd": meta.get('vsd', 'N/A'),
                    "lrg": meta.get('lrg', 'N/A'),
                    "Match Distance": round(results['distances'][0][i], 4)
                })
        
        # final_df = pd.DataFrame(hits)
        # # Ensure final DataFrame has all columns
        # for col in all_cols:
        #     if col not in final_df.columns:
        #         final_df[col] = "N/A"
        return hits

# --- EXECUTION ---
def test_vehicle_rates(): 
    # --- 1. SHARED TEST DATA ---
    MERCEDES_EQB_2023 = {'make': 'MERCEDES-BENZ', 
                         'model': 'EQB-Class', 
                         'year': 2023, 
                         'style': 'Sport Utility Vehicle (SUV)/Multi-Purpose Vehicle (MPV)', 
                         'engine': 'ELECTRIC', # Added engine
                         'drive_type': 'AWD/All-Wheel Drive', 
                         'doors': 4, 
                         'trim': 'EQB300 4MATIC'
                         }
    
    JSON_VEHICLE_TEST = { "make": "MERCEDES BENZ",
      "model": "EQB CLASS EV 300",
      "modelYear": 2023,
      "style": "4D 4WD 300",
      "engine": "ELECTRIC", # Added engine
      "vin": "W1N9M0KB6PN066911",
      "garagingZipCode": "95134",
      "primaryUse": "Other Use",
      "annualMiles": "13,000"}

    FIELDS_TO_DISPLAY = ['Year', 'Make', 'Model', 'series','package', 'style', 'engine', 'Match Distance','grg','drg','vsd','lrg'] 
    SCORING_RECOMMENDATIONS = " Match with Year Make Model should be boosted when scoring"

    MONGO_CONNECTION_URI = "mongodb://localhost:27017/" # Or your Atlas string
    MONGO_DB_NAME = "coveragecompassai"
    MONGO_COLLECTION_NAME = "vehicle-rates"
    MONGO_REBUILD_DB = False 
     # --- CSV Configuration ---
    RATINGS_FILE = "/Users/zubeydeyararbas/ml/insurance-quotes/Data/California/STATEFARM_CA_Insurance__tables/car_factors/vehicle_ratings_groups - Sheet1.csv"
    abstract_db_folder = "./vehicle_rates_csv"
    rates_app = VehicleRatesVectorDB( 
        db_folder=abstract_db_folder     
    )

    custom_weights_vin = {'make': 1.0, 'model': 2.0, 'year': 1.0, 'trim': 1.5, 'style': 0.1, 'engine': 0.5}
    print(f"Using custom weights: {custom_weights_vin}")
    
    print("\n---  Test 1: VIN Data Match Mercedes EQB-Class (Boosted) ---")
    results1 = rates_app.search_by_vin_data(MERCEDES_EQB_2023, boosting=True,boost_weights=custom_weights_vin)
    if not results1.empty:
        print(results1[FIELDS_TO_DISPLAY])
    else:
        print("No matches found.")
        
    RIVIAN_R1S_2025 = {
        'make': 'RIVIAN',
        'model': 'R1S',
        'year': 2025,  # Future year
        'style': 'Sport Utility Vehicle',
        'engine': 'ELECTRIC',
        'vin': "W1N9M0KB6PN066911",
        'garagingZipCode': "95134",
        'primaryUse': "Other Use",
        'annualMiles': "13,000"
    }

    print(f"RIVIAN Using custom weights: {custom_weights_vin}")
    resultsRivian = rates_app.search_by_vin_data(RIVIAN_R1S_2025, boosting=True, boost_weights=custom_weights_vin)
    if not resultsRivian.empty:
        print(resultsRivian[FIELDS_TO_DISPLAY])
    else:
        print("No matches found.")
  
# GRG (Comprehensive Rating Group): Groups vehicles for comprehensive (theft, fire, hail) coverage, often based on how likely they are to be stolen or damaged by non-collision events.
# DRG (Collision Rating Group): Groups vehicles for collision coverage, considering crashworthiness and repair costs, says.
# VSD (Vehicle Safety Discounts): Identifies vehicles that qualify for discounts due to built-in safety features, according to American Property Casualty Insurance Association and State Farm.
# LRG (Liability Rating Group): Groups vehicles for liability coverage (bodily injury/property damage), factoring in crash frequency and severity, according to American Property Casualty Insurance Association and State Farm. 

# -------------------------------------------------
# 3. SINGLETON INSTANCE & ACCESSORS
# -------------------------------------------------

_vehicle_rates_db_instance: Optional['VehicleRatesVectorDB'] = None

def initialize_vehicle_rates_db(db_folder: str = "./vehicle_rates_rag") -> None:
    """
    Initialize the global VehicleRatesVectorDB instance.
    Should be called once at application startup.
    """
    global _vehicle_rates_db_instance
    if _vehicle_rates_db_instance is None:
        _vehicle_rates_db_instance = VehicleRatesVectorDB(db_folder=db_folder)
        print("Global VehicleRatesVectorDB initialized.")
    else:
        print("Global VehicleRatesVectorDB already initialized.")

def get_vehicle_rates_db() -> 'VehicleRatesVectorDB':
    """
    Get the global VehicleRatesVectorDB instance.
    Raises RuntimeError if not initialized.
    """
    global _vehicle_rates_db_instance
    if _vehicle_rates_db_instance is None:
        # Fallback for scripts or tests that might not have called initialize
        # But in production, initialize should be called explicitly
        print("Warning: VehicleRatesVectorDB lazy initialization triggered.")
        initialize_vehicle_rates_db()
        
    return _vehicle_rates_db_instance


def test_mercedes():
    db = get_vehicle_rates_db() 
    vin_data = {'year': 2015, 
    'make': 'MERCEDES-BENZ', 
    'model': 'C-Class', 
    'trim': '4-MATIC', 
    'body_class': 'Sedan/Saloon', 
    'drive_type': 'AWD/All-Wheel Drive', 
    'engine_config': 'In-Line', 
    'doors': 4
    }
    results = db.search_by_vin_data(vin_data)
    print(results)