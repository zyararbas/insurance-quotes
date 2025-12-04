import pandas as pd
import chromadb
from tqdm import tqdm

def build_database():
    client = chromadb.PersistentClient(path="./vehicle_db")
    try:
        client.delete_collection("vehicles")
    except:
        pass
    collection = client.create_collection(name="vehicles")
    RATINGS_FILE = "/Users/zubeydeyararbas/ml/insurance-quotes/Data/California/STATEFARM_CA_Insurance__tables/car_factors/vehicle_ratings_groups - Sheet1.csv"
 
    # Load and CLEAN column names
    df = pd.read_csv(RATINGS_FILE).fillna('')
    df.columns = df.columns.str.strip()  # <--- FIX: Removes spaces from 'lrg ' and 'code '
    
    print(f"Columns found: {df.columns.tolist()}") # Confirm they are clean
    
    ids = []
    documents = []
    metadatas = []
    
    for idx, row in df.iterrows():
        # Text for matching
        text = f"{row['year']} {row['make']} {row['model']} {row['series']} {row['style']} {row['engine']}"
        
        ids.append(str(idx))
        documents.append(text)
        # The entire row (including drg, grg, vsd, lrg) is saved here
        metadatas.append(row.to_dict())

    # Batch add (same as before)
    batch_size = 1000
    for i in tqdm(range(0, len(documents), batch_size)):
        collection.add(
            ids=ids[i : i+batch_size],
            documents=documents[i : i+batch_size],
            metadatas=metadatas[i : i+batch_size]
        )
    print("Database built successfully.")

if __name__ == "__main__":
    build_database()