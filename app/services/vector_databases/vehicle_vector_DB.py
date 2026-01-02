import os
import pandas as pd
import chromadb
import shutil
import time
import ast
import re
from chromadb.utils import embedding_functions

# Define "junk" words that don't help identify a model
MODEL_STOP_WORDS = {
    'class', 'series', 'sedan', 'suv', 'coupe', 'ev', 'hybrid', 
    'benz', 'na', 'n', 'a' # 'na', 'n', 'a' handle 'N/A' after tokenizing
}

class VehicleVectorDB:
    def __init__(self, ratings_csv_path, db_folder="./vehicle_rates_rag", force_reindex=False):
        """
        Initializes the Local Vector Database.
        force_reindex: Set to True to delete the old DB and rebuild it.
        """
        print(f"--- Initializing ChromaDB (v{chromadb.__version__}) ---")
        
        # 1. Clean up old DB if requested
        if force_reindex and os.path.exists(db_folder):
            print(f"[RESET] Deleting old database at {db_folder} to force re-indexing...")
            shutil.rmtree(db_folder)
            time.sleep(1) # Pause to let OS release file locks
            
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
        
        # 5. Check if we need to index data
        if self.collection.count() == 0:
            print("Database is empty. Indexing your CSV file now...")
            self._index_data(ratings_csv_path)
        else:
            print(f"Database loaded! Contains {self.collection.count()} vehicles.")

    def _index_data(self, csv_path):
        """
        Reads the CSV and saves to ChromaDB in BATCHES to avoid memory errors.
        """
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Could not find file: {csv_path}")

        df = pd.read_csv(csv_path)
        
        documents = []
        metadatas = []
        ids = []
        
        print(f"Preparing {len(df)} rows for indexing...")
        
        for idx, row in df.iterrows():
            # Helper to safely get string values
            def clean(val): return str(val).strip() if pd.notna(val) else "N/A"

            # Create semantic string
            semantic_text = (
                f"{row['year']} {clean(row['make'])} {clean(row['model'])} {clean(row.get('series'))} {clean(row.get('package'))} {clean(row['style'])}. "
                f"Engine: {clean(row.get('engine'))}. "
                f"Ratings - GRG: {row['grg']}, DRG: {row['drg']}."
            )
            
            documents.append(semantic_text)
            
            # Store Metadata
            # Using .get() for safety if columns are missing in CSV
            metadatas.append({
                "year": int(row['year']),
                "make": clean(row['make']),
                "model": clean(row['model']),
                "series": clean(row.get('series')),
                "package": clean(row.get('package')),
                "style": clean(row['style']),
                "engine": clean(row.get('engine')),
                "grg": int(row['grg'])
            })
            ids.append(str(idx))
            
        # Batch Insertion
        BATCH_SIZE = 2000
        total_docs = len(documents)
        
        print(f"Starting batch insertion (Batch Size: {BATCH_SIZE})...")
        
        for i in range(0, total_docs, BATCH_SIZE):
            end_idx = min(i + BATCH_SIZE, total_docs)
            self.collection.add(
                documents=documents[i:end_idx],
                metadatas=metadatas[i:end_idx],
                ids=ids[i:end_idx]
            )
            print(f"  - Indexed {end_idx}/{total_docs}")
            
        print("Indexing Complete.")

    def _tokenize_model(self, model_name):
        """
        Helper to robustly tokenize model/trim names and remove stop words.
        Splits letters from numbers (e.g., 'EQB300' -> 'eqb 300').
        """
        # Insert space between letters and numbers
        s = re.sub(r'([a-zA-Z])([0-9])', r'\1 \2', model_name)
        s = re.sub(r'([0-9])([a-zA-Z])', r'\1 \2', s)
        
        # Replace hyphens/slashes with spaces, strip non-alphanumeric, lowercase
        tokens = re.sub(r'[^a-z0-9\s]+', '', s.lower().replace('-', ' '))
        
        # Split by space, filter out empty tokens and stop words
        return set(filter(lambda t: t and t not in MODEL_STOP_WORDS, tokens.split()))

    def _jaccard_similarity(self, set_a, set_b):
        """Calculates Jaccard similarity between two sets."""
        if not set_a and not set_b:
            return 0.0 # Avoid division by zero if both are empty after filtering
            
        intersection = set_a.intersection(set_b)
        union = set_a.union(set_b)
        
        if not union:
             return 0.0 # Avoid division by zero
             
        return len(intersection) / len(union)

    def search_with_boosting(self, query_text, boost_targets, weights, top_k=5):
        """
        Performs Vector Search + Jaccard Similarity Boosting (Re-ranking).
        Now boosts: Make, Model, Year, Trim (combined series/package)
        """
        # 1. Fetch Wide (Top 50)
        results = self.collection.query(
            query_texts=[query_text],
            n_results=50 
        )
        
        reranked_candidates = []
        
        # 2. Apply Boosts
        if results['ids'] and len(results['ids']) > 0:
            # Pre-tokenize targets once
            target_make_tokens = self._tokenize_model(boost_targets.get('make', ''))
            target_model_tokens = self._tokenize_model(boost_targets.get('model', ''))
            target_trim_tokens = self._tokenize_model(boost_targets.get('trim', ''))
            
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

                # C. Year Boost
                target_year = int(boost_targets.get('year', 0))
                candidate_year = int(meta.get('year', 0))
                if target_year > 0:
                    if candidate_year == target_year:
                        new_score -= weights.get('year', 0.0)
                    elif abs(candidate_year - target_year) <= 2:
                        # Apply partial boost for "near" years
                        new_score -= (weights.get('year', 0.0) / 2)

                # D. Trim Boost (Proportional Jaccard Similarity)
                # This logic combines the db's series and package to match the user's trim
                candidate_series = meta.get('series', '')
                candidate_package = meta.get('package', '')
                candidate_full_trim = f"{candidate_series} {candidate_package}"
                
                candidate_trim_tokens = self._tokenize_model(candidate_full_trim)
                
                jaccard_score_trim = self._jaccard_similarity(target_trim_tokens, candidate_trim_tokens)
                new_score -= (weights.get('trim', 0.0) * jaccard_score_trim)

                reranked_candidates.append({
                    "Vehicle Info": results['documents'][0][i],
                    "year": meta.get('year'),
                    "make": meta.get('make'),
                    "model": meta.get('model'),
                    "series": meta.get('series', 'N/A'),
                    "package": meta.get('package', 'N/A'),
                    "style": meta.get('style', 'N/A'),
                    "engine": meta.get('engine', 'N/A'),
                    "Match Distance": round(new_score, 4)  # Boosted Score
                })
        
        # 3. Sort & Slice
        reranked_candidates.sort(key=lambda x: x['Match Distance'])
        
        # Format to match _format_results output keys
        formatted_hits = []
        for item in reranked_candidates[:top_k]:
            formatted_hits.append({
                "Vehicle Info": item['Vehicle Info'],
                "Year": item['year'],
                "Make": item['make'],
                "Model": item['model'],
                "series": item['series'],
                "package": item['package'],
                "style": item['style'],
                "engine": item['engine'],
                "Match Distance": item['Match Distance']
            })
            
        return pd.DataFrame(formatted_hits)

    def search_by_text(self, query_text, top_k=5, boost_weights=None):
        """
        Standard semantic search. 
        Detects if the query string contains boosting instructions and a JSON dict.
        Can also accept an external boost_weights dictionary.
        """
        # Check for boosting instruction
        if "should be boosted" in query_text:
            try:
                # Attempt to find a dictionary-like structure { ... }
                match = re.search(r"\{.*\}", query_text)
                if match:
                    dict_str = match.group(0)
                    vehicle_dict = ast.literal_eval(dict_str)
                    
                    # Map dictionary keys to our database metadata fields
                    boost_targets = {}
                    # Handle common keys from user input
                    if 'modelYear' in vehicle_dict: boost_targets['year'] = int(vehicle_dict['modelYear'])
                    if 'year' in vehicle_dict: boost_targets['year'] = int(vehicle_dict['year'])
                    if 'make' in vehicle_dict: boost_targets['make'] = vehicle_dict['make']
                    if 'model' in vehicle_dict: boost_targets['model'] = vehicle_dict['model']
                    
                    # Updated mapping: 'trim' from dict maps to 'trim' for boosting
                    if 'trim' in vehicle_dict: boost_targets['trim'] = vehicle_dict['trim']
                    elif 'bodyType' in vehicle_dict: boost_targets['trim'] = vehicle_dict['bodyType'] # Fallback
                    
                    # Define weights for boosting
                    # Use provided weights or default (now includes trim)
                    weights = boost_weights if boost_weights is not None else {'make': 0.5, 'model': 0.3, 'year': 0.1, 'trim': 0.2}
                    
                    print(f"\n[Boosting] Detected targets: {boost_targets} with weights: {weights}")
                    return self.search_with_boosting(query_text, boost_targets, weights, top_k)
            except Exception as e:
                print(f"[Warning] Boosting parsing failed: {e}. Proceeding with standard search.")

        # Fallback to standard search
        results = self.collection.query(
            query_texts=[query_text],
            n_results=top_k
        )
        return self._format_results(results)

    def search_by_vin_data(self, vin_data, boosting=True, boost_weights=None):
        """
        Searches the DB based on a VIN data dictionary.
        
        Args:
            vin_data (dict): A dictionary containing vehicle specs.
            boosting (bool): Whether to apply boosting logic.
            boost_weights (dict): A dictionary of {field: factor} to control boosting.
        """
        
        # 1. Extract specs from dictionary
        year_str = str(vin_data.get('year', ''))
        make = vin_data.get('make', '')
        model = vin_data.get('model', '')
        # Map 'body_class' from your example, fallback to 'style'
        style = vin_data.get('body_class', vin_data.get('style', '')) 
        trim = vin_data.get('trim', '') # Get trim

        # 2. Construct Semantic Query (now includes trim)
        query = f"{year_str} {make} {model} {style} {trim}"
        
        # 3. Set up boosting
        if boosting:
            # Define Boost Targets
            boost_targets = {
                'year': int(year_str) if year_str.isdigit() else 0,
                'make': make,
                'model': model,
                'trim': trim # Map 'trim' from vin_data to 'trim' for boosting
            }
            
            # Define Weights: Use provided or default (now includes trim)
            weights = boost_weights if boost_weights is not None else {'make': 0.5, 'model': 0.3, 'year': 0.15, 'trim': 0.2}
            
            print(f"\nUsing Search Query: '{query}' (with boosting: {weights})")
            return self.search_with_boosting(query, boost_targets, weights)
        else:
            # Run a standard, non-boosted search
            print(f"\nUsing Search Query: '{query}' (boosting disabled)")
            results = self.collection.query(
                query_texts=[query],
                n_results=5
            )
            return self._format_results(results)

    def _format_results(self, results):
        hits = []
        if results['ids'] and len(results['ids']) > 0:
            for i in range(len(results['ids'][0])):
                meta = results['metadatas'][0][i]
                hits.append({
                    "Vehicle Info": results['documents'][0][i],
                    "Year": meta.get('year'),
                    "Make": meta.get('make'),
                    "Model": meta.get('model'),
                    "series": meta.get('series', 'N/A'),
                    "package": meta.get('package', 'N/A'),
                    "style": meta.get('style', 'N/A'), 
                    "engine": meta.get('engine', 'N/A'), 
                    "Match Distance": round(results['distances'][0][i], 4)
                })
        return pd.DataFrame(hits)

# --- EXECUTION ---

MERCEDES_EQB_2023 = {'make': 'MERCEDES-BENZ', 
                     'model': 'EQB-Class', 
                     'year': 2023, 
                     'body_class': 'Sport Utility Vehicle (SUV)/Multi-Purpose Vehicle (MPV)', 
                     'drive_type': 'AWD/All-Wheel Drive', 
                     'doors': 4, 
                     'trim': 'EQB300 4MATIC'
                     }

if __name__ == "__main__":
    # 1. Initialize
    app = VehicleVectorDB("/Users/zubeydeyararbas/ml/insurance-quotes/Data/California/STATEFARM_CA_Insurance__tables/car_factors/auto_ratings_2024_2001.csv")
    fields = ['MAKE', 'MODEL', 'SERIES','OPTIONPACKAGE', 'BODYSTYLE','MATCH DISTANCE','YEAR'] 
    scoring_recommendations = " Match with Year Make Model should be boosted when scoring"

    # 2. Test: Semantic Search
    print("\n--- Test 1: User Question ---")
    question = "Find me a safe hybrid Audi sedan"
    print(f"Asking: '{question}'...")
    print(app.search_by_text(question)[fields])

    # 3. Test: Search by JSON (Text Search with Custom Boosting)
    vehicle = { "make": "MERCEDES BENZ",
      "model": "EQB CLASS EV 300",
      "modelYear": 2023,
      "bodyType": "4D 4WD 300",
      "vin": "W1N9M0KB6PN066911",
      "garagingZipCode": "95134",
      "primaryUse": "Other Use",
      "annualMiles": "13,000"}
    question = f"Find me exact or similar vehicles like this {vehicle} {scoring_recommendations}"
    
    # Define custom weights for this text search
    custom_weights_text = {'make': 0.1, 'model': 0.1, 'year': 0.8, 'trim': 0.1} # Heavily boost YEAR
    
    print(f"\n--- Test 2: Search by JSON (Text Search with Custom Boosting) ---")
    print(f"Asking: '{question}'...")
    print(f"Using custom weights: {custom_weights_text}")
    print(app.search_by_text(question, boost_weights=custom_weights_text)[fields])
    
    # 4. Test: Search by VIN Data (Default Boosting)
    print("\n--- Test 3: VIN Data Match Mercedes EQB-Class (Default Boosting) ---")
    results2 = app.search_by_vin_data(MERCEDES_EQB_2023, boosting=True)
    if not results2.empty:
        print(results2[fields])
    else:
        print("No matches found (or file error occurred). Check error messages above.")
        
    # 5. Test: Search by VIN Data (Custom Boosting)
    print("\n--- Test 4: VIN Data Match Mercedes EQB-Class (Custom Boosting) ---")
    # This is the test case from your prompt, now using 'trim'
    custom_weights_vin = {'year': 0.7, 'make': 0.6, 'model': 0.5,  'series': 0.4, 'trim': 0.3}
    print(f"Using custom weights: {custom_weights_vin}")
    results3 = app.search_by_vin_data(MERCEDES_EQB_2023, boosting=True, boost_weights=custom_weights_vin)
    if not results3.empty:
        print(results3[fields])
    else:
        print("No matches found.")