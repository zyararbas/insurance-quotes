import os
import pandas as pd

class CsvDataLoader():
    """Loads vehicle data from a CSV file."""
    
    def __init__(self, csv_path):
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Could not find CSV file at path: {csv_path}")
        self.csv_path = csv_path

    def index_data(self, collection):
        """
        Reads data from the CSV file and indexes it into ChromaDB.
        """
        df = pd.read_csv(self.csv_path)
        
        documents = []
        metadatas = []
        ids = []
        
        print(f"Preparing {len(df)} rows from CSV for indexing...")
        
        for idx, row in df.iterrows():
            def clean(val): return str(val).strip() if pd.notna(val) else "N/A"

            # Create semantic string
            semantic_text = (
                f"{row.get('year')} {clean(row.get('make'))} {clean(row.get('model'))} {clean(row.get('series'))} {clean(row.get('package'))} {clean(row.get('style'))}. "
                f"Engine: {clean(row.get('engine'))}. "
                f"Ratings - GRG: {row.get('grg')}, DRG: {row.get('drg')}."
            )
            documents.append(semantic_text)
            
            # Store metadata with lowercase keys
            metadatas.append({
                "year": int(row.get('year', 0)),
                "make": clean(row.get('make')),
                "model": clean(row.get('model')),
                "series": clean(row.get('series')),
                "package": clean(row.get('package')),
                "style": clean(row.get('style')),
                "engine": clean(row.get('engine')),
                "grg": int(row.get('grg', 0))
            })
            ids.append(f"csv_{idx}") # Create a unique ID for CSV rows
            
        # Batch Insertion
        BATCH_SIZE = 2000
        total_docs = len(documents)
        
        print(f"Starting batch insertion (Batch Size: {BATCH_SIZE})...")
        
        for i in range(0, total_docs, BATCH_SIZE):
            end_idx = min(i + BATCH_SIZE, total_docs)
            collection.add(
                documents=documents[i:end_idx],
                metadatas=metadatas[i:end_idx],
                ids=ids[i:end_idx]
            )
            print(f"  - Indexed {end_idx}/{total_docs}")
            
        print("CSV Indexing Complete.")