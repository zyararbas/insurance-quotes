import os
import pandas as pd
import chromadb
import shutil
import time
import re
import ast
from chromadb.utils import embedding_functions
from typing import  Optional 
MODEL_STOP_WORDS = {
    'class', 'series', 'sedan', 'suv', 'coupe', 'ev', 'hybrid', 
    'benz', 'na', 'n', 'a', '-', 'dr', '4d', '2d'
}

class VehicleRatesChromaDB:
    def __init__(self, ratings_csv_path, db_folder="./vehicle_rates_chroma_db", force_reindex=False):
        """
        Initializes the VehicleRatesChroma Database.
        """
        print(f"--- Initializing VehicleRatesChroma (v{chromadb.__version__}) ---")
        
        # 1. Clean up old DB if requested
        if force_reindex and os.path.exists(db_folder):
            print(f"[RESET] Deleting old database at {db_folder} to force re-indexing...")
            shutil.rmtree(db_folder)
            time.sleep(1) 
            
        # 2. Connect to local persistent storage
        self.client = chromadb.PersistentClient(path=db_folder)
        
        # 3. Define the embedding model
        self.emb_fn = embedding_functions.SentenceTransformerEmbeddingFunction(
            model_name="all-mpnet-base-v2"
        )
        
        # 4. Get or Create the Collection
        self.collection = self.client.get_or_create_collection(
            name="vehicle_rates_collection",
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
        Reads the CSV and saves to ChromaDB in BATCHES.
        """
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Could not find file: {csv_path}")

        print(f"Reading CSV from: {csv_path}")
        df = pd.read_csv(csv_path)
        
        documents = []
        metadatas = []
        ids = []
        
        print(f"Preparing {len(df)} rows for indexing...")
        
        for idx, row in df.iterrows():
            # Helper to safely get string values and NORMALIZE hyphens
            def clean(val): 
                if pd.isna(val): return "N/A"
                s = str(val).strip()
                # Normalize special hyphens to standard ASCII hyphen
                s = s.replace('‐', '-') 
                return s
            
            # Create semantic string used for embedding
            semantic_text = (
                f"Year: {row.get('YEAR')} "
                f"Make: {clean(row.get('MAKE'))} " 
                f"Model: {clean(row.get('MODEL'))} "
                f"Series: {clean(row.get('SERIES'))} " 
                f"Option: {clean(row.get('OPTIONPACKAGE'))} " 
                f"Body: {clean(row.get('BODYSTYLE'))} "
                f"Engine: {clean(row.get('ENGINE'))} " 
                f"Wheelbase: {clean(row.get('WHEELBASE', clean(row.get('Wheelbase'))))} " # Handle casing
            )
            
            documents.append(semantic_text)
            
            # Store Metadata for retrieval and filtering if needed
            metadatas.append({
                "year": int(row['YEAR']) if pd.notna(row.get('YEAR')) else 0,
                "make": clean(row.get('MAKE')),
                "model": clean(row.get('MODEL')),
                "series": clean(row.get('SERIES', '')),
                "package": clean(row.get('OPTIONPACKAGE', '')),
                "style": clean(row.get('BODYSTYLE', '')),
                "engine": clean(row.get('ENGINE', '')),  
                "wheelbase": clean(row.get('WHEELBASE', clean(row.get('Wheelbase', '')))),
                # Store ratings in metadata for easy access
                "grg": str(row.get('grg', row.get('GRG', ''))),
                "drg": str(row.get('drg', row.get('DRG', ''))),
                "vsd": str(row.get('vsd', row.get('VSD', ''))),
                "lrg": str(row.get('lrg', row.get('LRG', '')))
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
        if not model_name: return set()
        # Insert space between letters and numbers
        s = re.sub(r'([a-zA-Z])([0-9])', r'\1 \2', str(model_name))
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

    def query_vehicles(self, query_text, where_clause=None, n_results=5, boost_targets=None, boost_weights=None):
        """
        Queries the database for the best matching vehicles.
        
        Args:
            query_text (str): The semantic query string.
            where_clause (dict): Optional filtering clause (e.g. {'make': 'BMW'}).
            n_results (int): Number of final results to return.
            boost_targets (dict): Optional dictionary of target values for boosting (e.g. {'year': 2020, 'make': 'BMW'}).
            boost_weights (dict): Optional weights for boosting (e.g. {'year': 0.5}).
        """
        print(f"Querying for: '{query_text}'")
        
        # 1. Fetch more results if boosting is enabled to allow for re-ranking
        fetch_k = n_results * 10 if boost_targets else n_results
        
        results = self.collection.query(
            query_texts=[query_text],
            n_results=fetch_k,
            where=where_clause  
        )
        
        hits = []
        if results['ids'] and len(results['ids']) > 0:
            
            # Pre-tokenize targets if boosting
            if boost_targets:
                target_make_tokens = self._tokenize_model(boost_targets.get('make', ''))
                target_model_tokens = self._tokenize_model(boost_targets.get('model', ''))
                target_trim_tokens = self._tokenize_model(boost_targets.get('trim', ''))
                target_style_tokens = self._tokenize_model(boost_targets.get('style', ''))
                
                # Default weights if not provided
                if not boost_weights:
                    boost_weights = {'make': 2.0, 'model': 1.0, 'year': 0.5, 'trim': 0.5, 'style': 0.5}

            for i in range(len(results['ids'][0])):
                meta = results['metadatas'][0][i]
                dist = results['distances'][0][i]
                
                # --- BOOSTING LOGIC ---
                new_score = dist
                if boost_targets:
                     # A. Make Boost
                    candidate_make_tokens = self._tokenize_model(meta.get('make', ''))
                    jaccard_score_make = self._jaccard_similarity(target_make_tokens, candidate_make_tokens)
                    new_score -= (boost_weights.get('make', 0.0) * jaccard_score_make)
                    
                    # B. Model Boost
                    candidate_model_tokens = self._tokenize_model(meta.get('model', ''))
                    jaccard_score_model = self._jaccard_similarity(target_model_tokens, candidate_model_tokens)
                    new_score -= (boost_weights.get('model', 0.0) * jaccard_score_model)

                    # C. Year Boost
                    target_year = int(boost_targets.get('year', 0))
                    candidate_year = int(meta.get('year', 0))
                    if target_year > 0 and candidate_year > 0:
                        if candidate_year == target_year:
                            new_score -= boost_weights.get('year', 0.0)
                        elif abs(candidate_year - target_year) <= 2:
                            new_score -= (boost_weights.get('year', 0.0) / 2)
                            
                    # D. Trim/Style/Description Boost
                    # We compare target 'trim' against candidate 'description' (which usually contains trim info) or 'style'
                    # Construct a combined string from metadata to check against trim
                    candidate_desc_tokens = self._tokenize_model(meta.get('series', '') + ' ' + meta.get('package', '') + ' ' + meta.get('style', ''))
                    jaccard_score_trim = self._jaccard_similarity(target_trim_tokens, candidate_desc_tokens)
                    new_score -= (boost_weights.get('trim', 0.0) * jaccard_score_trim)
                ## YEAR YEAR YEAR IF NOT FOUND THEN SET TO YEAR THE SAME SO AI DOES NOT ASK WHICH YEAR
                hits.append({
                    "Vehicle Info": results['documents'][0][i], # Vehicle Info
                    "year":  boost_targets.get('year', meta.get('year')),
                    "make": meta.get('make'), # make
                    "model": meta.get('model'), # model
                    "series": meta.get('series'), # series
                    "trim": meta.get('trim'), # trim
                    "style": meta.get('style'), # style
                    "engine": meta.get('engine'), # engine
                    "wheelbase": meta.get('wheelbase'), # wheelbase
                    "grg": meta.get('grg'), # grg   
                    "drg": meta.get('drg'), # drg
                    "vsd": meta.get('vsd'), # vsd
                    "lrg": meta.get('lrg'), # lrg
                    "Match Score": round(new_score, 4), 
                    "Original Distance": round(dist, 4),
                })
        
        # Sort by new score (lower is better in distance-based, but we subtracted boosts, so lower is still better)
        # Using L2 distance, smaller is better. Subtracting positive boost makes it smaller (better).
        hits.sort(key=lambda x: x['Match Score'])
        
        return hits[:n_results]

    def search_by_vin_data(self, vin_data, boosting=True, boost_weights=None):
        """
        Searches the DB based on a VIN data dictionary.
        """
        year_str = str(vin_data.get('year', ''))
        make = vin_data.get('make', '')
        model = vin_data.get('model', '')
        style = f"{vin_data.get('body_class')} {vin_data.get('style', '')} {(vin_data.get('doors', ''))}D"
        style = style.replace("SED", "sedan")
        series = vin_data.get('trim', '')
        engine = vin_data.get('engine', '') 

        query = f"""
        Find me exact or similar vehicles like this year: {year_str} make: {make} model: {model} style: {style} series: {series} engine: {engine} doors: {vin_data.get('doors', '')},  
                make sure both make and model match ,
                if there is a different make or model, if both do not match your critierie then ignore
        """
        
        if boosting:
            boost_targets = {
                'year': int(year_str) if year_str.isdigit() else 0,
                'make': make,
                'model': model,
                'series': series,
                'style': style,
                'engine': engine
            }
            weights = boost_weights if boost_weights is not None else {
                'make': 2.0,
                'model': 1.0, 
                'year': 1.0, 
                'series': 0.5,
                'style': 0.5,
            }

            print(f"\nUsing Search Query: '{query}' (with boosting: {weights})")
            
            # Use strict filtering for Make and Model
            where = {"make": make} if make else None
           
            return self.query_vehicles(
                query, 
                where_clause=where,
                n_results=5,
                boost_targets=boost_targets,
                boost_weights=weights 
            )

    
# -------------------------------------------------
# 3. SINGLETON INSTANCE & ACCESSORS
# -------------------------------------------------

_vehicle_rates_chromadb_instance: Optional['VehicleRatesChromaDB'] = None


def initialize_vehicle_rates_chromadb() -> None:
    """
    Initialize the global VehicleRatesChroma instance.
    Should be called once at application startup.
    """
    global _vehicle_rates_chromadb_instance
    if _vehicle_rates_chromadb_instance is None:
        _vehicle_rates_chromadb_instance = VehicleRatesChromaDB(ratings_csv_path=None, db_folder="./vehicle_rates_chroma_db", force_reindex=False)
        print("Global VehicleRatesChromaDB initialized.")
    else:
        print("Global VehicleRatesChromaDB already initialized.")

def get_vehicle_rates_chromadb() -> 'VehicleRatesChromaDB':
    """
    Get the global VehicleRatesChromaDB instance.
    Raises RuntimeError if not initialized.
    """
    global _vehicle_rates_chromadb_instance
    if _vehicle_rates_chromadb_instance is None:
        # Fallback for scripts or tests that might not have called initialize
        # But in production, initialize should be called explicitly
        print("Warning: VehicleRatesChroma lazy initialization triggered.")
        initialize_vehicle_rates_chromadb()
        
    return _vehicle_rates_chromadb_instance 

if __name__ == "__main__":
    # Example Usage
    # We use the path we know exists from the context
    CSV_PATH = "/Users/zubeydeyararbas/ml/insurance-quotes/Data/California/STATEFARM_CA_Insurance__tables/car_factors/auto_ratings_2024_2001.csv"
    
    # 1. Initialize
    db = VehicleRatesChromaDB(ratings_csv_path=CSV_PATH)
    
    # 2. Test Query
    query = "2015 MERCEDES-BENZ 4-MATIC C-Class 4D 2.0"
    matches = db.query_vehicles(query)
    
    print("\n--- Top Matches ---")
    for m in matches:
        print(m)


     # 3. Test Query Audi
    query = "2011 AUDI Premium Plus Q5 Sport Utility Vehicle (SUV)/Multi-Purpose Vehicle (MPV) 4D 3.2"
    matches = db.query_vehicles(query)
    
    print("\n--- Top Matches ---")
    for m in matches:
        print(m)

    
    # 4. Test Query with Boosting Parameters
    print("\n--- Test Query with Boosting Parameters ---")
    query_text = "2015 MERCEDES-BENZ C-Class Sedan 4-MATIC"
    
    # Define explicit targets for boosting
    boost_targets = {
        'year': 2015,
        'make': 'MERCEDES-BENZ',
        'model': 'C-Class',
        'trim': '4-MATIC'
    }
    
    # Optional: Define custom weights
    boost_weights = {
        'make': 2.0,
        'model': 1.0, 
        'year': 1.0, 
        'trim': 0.5
    }
    
    matches = db.query_vehicles(query_text, n_results=5, boost_targets=boost_targets, boost_weights=boost_weights)
    
    print("\n--- Top Matches (Boosted) ---")
    for m in matches:
        print(m)


    boost_weights = {
        'make': 2.0,
        'model': 1.0, 
        'year': 1.0, 
        'series': 0.5,
        'style': 0.5,
    }
    boost_targets = {
        'year': 2015,
        'make': 'MERCEDES-BENZ',
        'model': 'C-Class',
        'trim': '4-MATIC',
        'style': 'Sedan/Saloon SED, 4 doors 4D', 
    }


    query_text = f"""
    Find me exact or similar vehicles like  this
     year 2015 make: MERCEDES-BENZ model C-Class, style: Sedan/Saloon SED, 4 doors 4D ,trim: 4-MATIC engine 
     make sure both make and model match ,if there is a different make or model, 
     if both do not match your critierie then ignore
    """
    where_clause= {'make': 'MERCEDES-BENZ'}
   
    matches = db.query_vehicles(query_text,where_clause, boost_weights=boost_weights, boost_targets=boost_targets)
    
    print("\n--- Top Matches ---")
    for m in matches:
        print(m)


    vin_data = {
        'make': 'RIVIAN', 
        'model': 'R1S', 
        'year': 2025, 
        'trim': 'Adventure', 
        'body_class': 'Sport Utility Vehicle (SUV)/Multi-Purpose Vehicle (MPV)', 
        'drive_type': 'AWD/All-Wheel Drive', 
        'transmission': 'Automatic', 
        'doors': 4
        }
    
    matches = db.search_by_vin_data(vin_data)
    
    print("\n--- Top Matches ---")
    for m in matches:
        print(m)