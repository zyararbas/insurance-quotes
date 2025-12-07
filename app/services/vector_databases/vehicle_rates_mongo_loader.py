import pandas as pd
from pymongo import MongoClient 

class MongoDataLoader():
    """Loads vehicle data from a MongoDB database."""
    
    def __init__(self, mongo_uri, db_name, collection_name):
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.mongo_client = None

    def index_data(self, collection):
        """
        Connects to MongoDB, reads the data, and saves to ChromaDB in BATCHES.
        Reads UPPERCASE keys from Mongo, saves as lowercase keys in Chroma.
        """
        try:
            self.mongo_client = MongoClient(self.mongo_uri)
            db = self.mongo_client[self.db_name]
            mongo_coll = db[self.collection_name]
            cursor = mongo_coll.find()
            total_docs = mongo_coll.count_documents({})
            print(f"Found {total_docs} documents in MongoDB. Preparing for indexing...")

        except Exception as e:
            if self.mongo_client:
                self.mongo_client.close()
            raise ConnectionError(f"Could not connect to or read from MongoDB: {e}")

        
        documents = []
        metadatas = []
        ids = []
        
        for row in cursor:
            def clean(val): return str(val).strip() if pd.notna(val) else "N/A"

            # --- UPDATED: Read ALL UPPERCASE keys from MongoDB ---
            semantic_text = (
                f"{clean(row.get('YEAR'))} {clean(row.get('MAKE'))} {clean(row.get('MODEL'))} "
                f"{clean(row.get('SERIES'))} {clean(row.get('OPTIONPACKAGE'))} {clean(row.get('BODYSTYLE'))}. "
                f"Engine: {clean(row.get('ENGINE'))}. "
                f"Ratings - GRG: {clean(row.get('GRG'))}, DRG: {clean(row.get('DRG'))}, "
                f"VSD: {clean(row.get('VSD'))}, LRG: {clean(row.get('LRG'))}."
            )
            documents.append(semantic_text)
            
            # --- UPDATED: Store all metadata with lowercase keys ---
            metadatas.append({
                "year": int(row.get('YEAR', 0)),
                "make": clean(row.get('MAKE')),
                "model": clean(row.get('MODEL')),
                "series": clean(row.get('SERIES')),
                "package": clean(row.get('OPTIONPACKAGE')), # Maps from OPTIONPACKAGE
                "style": clean(row.get('BODYSTYLE')),     # Maps from BODYSTYLE
                "engine": clean(row.get('ENGINE')),
                "grg": clean(row.get('GRG')), # Keep as string/int based on data
                "drg": clean(row.get('DRG')),
                "vsd": clean(row.get('VSD')),
                "lrg": clean(row.get('LRG')),
                "expiration": clean(row.get('EXPIRATION'))
            })
            ids.append(str(row['_id']))
            
        # Batch Insertion
        BATCH_SIZE = 2000
        print(f"Starting batch insertion (Batch Size: {BATCH_SIZE})...")
        
        for i in range(0, total_docs, BATCH_SIZE):
            end_idx = min(i + BATCH_SIZE, total_docs)
            collection.add(
                documents=documents[i:end_idx],
                metadatas=metadatas[i:end_idx],
                ids=ids[i:end_idx]
            )
            print(f"  - Indexed {end_idx}/{total_docs}")
            
        print("MongoDB Indexing Complete.")
        self.mongo_client.close()