import chromadb
import pandas as pd
import os

class VehicleRAG:
    def __init__(self, db_path="./vehicle_db"):
        # 1. Debug Path: Print where we are looking for the DB
        abs_path = os.path.abspath(db_path)
        print(f"DEBUG: Connecting to DB at {abs_path}")
        
        self.client = chromadb.PersistentClient(path=db_path)
        try:
            self.collection = self.client.get_collection(name="vehicles")
            count = self.collection.count()
            print(f"✅ Vehicle Database Loaded. Total Vehicles: {count}")
        except Exception as e:
            print(f"❌ Error loading collection: {e}")
            print(f"   (Make sure the folder '{abs_path}' exists and contains chroma.sqlite3)")
            self.collection = None

    def search(self, query_text, n_results=5):
        """
        Returns a LIST of dictionaries (not a DataFrame) to avoid ambiguity errors.
        """
        if not self.collection:
            print("⚠️ Warning: Database not loaded. Returning empty list.")
            return []

        print(f"DEBUG: Searching for '{query_text}'...")
        results = self.collection.query(
            query_texts=[query_text],
            n_results=n_results
        )
        
        structured_response = []
        
        # Check if we actually found matches
        if results['ids'] and results['ids'][0]:
            for i, _ in enumerate(results['ids'][0]):
                metadata = results['metadatas'][0][i]
                distance = results['distances'][0][i]
                
                # Build the Clean Object
                item = {
                    'year': metadata.get('year'),
                    'make': metadata.get('make'),
                    'model': metadata.get('model'),
                    'series': metadata.get('series'),
                    'style': metadata.get('style'),
                    'engine': metadata.get('engine'),
                    # Ratings
                    'drg': metadata.get('drg'),
                    'grg': metadata.get('grg'),
                    'vsd': metadata.get('vsd'),
                    'lrg': metadata.get('lrg'),
                    # Score
                    'Match Distance': round(distance, 4)
                }
                structured_response.append(item)
                
        return structured_response

    def generate_clarifying_question(self, matches):
        """
        Identifies ambiguous fields (excluding YEAR) and generates a question.
        """
        if not matches:
            return None, None

        # 1. Define fields to check for ambiguity
        # We explicitly EXCLUDE 'year' because we want to accept the "nearest" year found.
        candidate_fields = ['series', 'package', 'style', 'engine', 'drive_type']
        
        ambiguities = {}
        
        for field in candidate_fields:
            # Get unique values for this field across all matches
            values = set()
            for m in matches:
                val = m.get(field)
                # Filter out empty/nan values
                if val and str(val).strip() != '' and str(val).lower() != 'nan':
                    values.add(val)
            
            # If there is more than 1 option, it's ambiguous
            if len(values) > 1:
                ambiguities[field] = list(values)

        # 2. Construct the Question
        if not ambiguities:
            return None, None

        # Pick the most important field to clarify first (e.g., Series/Trim is usually key)
        priority_order = ['series', 'package', 'style', 'engine']
        target_field = next((f for f in priority_order if f in ambiguities), list(ambiguities.keys())[0])
        
        options = ambiguities[target_field]
        question = f"I found a few similar vehicles. Could you specify the **{target_field.upper()}**? \nOptions: {', '.join(options)}"
        
        return question, ambiguities

def construct_query(data):
    """
    Intelligently converts different input formats into a RAG search string.
    """
    parts = []
    
    # Handle Year
    if 'year' in data: parts.append(str(data['year']))
    elif 'modelYear' in data: parts.append(str(data['modelYear']))
    
    # Handle Make
    parts.append(data.get('make', ''))
    
    # Handle Model
    parts.append(data.get('model', ''))
    
    # Handle Series/Trim (Valuable for specific matching)
    if 'trim' in data: parts.append(data['trim'])
    
    # Handle Style
    parts.append(data.get('style', ''))
    
    # Handle Engine
    parts.append(data.get('engine', ''))
    
    # Clean up and join
    return " ".join([p for p in parts if p])


def analyzeMatches(matches):
     # 2. Analyze Matches
    if matches:
        # Get the best year found (just for display)
        found_year = matches[0]['year']
        print(f"✅ Nearest Year Found: {found_year} (Auto-selected)\n")
    
        # 3. Generate Clarification (Ignoring Year)
        question, options = rag.generate_clarifying_question(matches)
        if question:
            generateClarificationQuestion(question, options)
        else:
            print("✅ Exact match found (or no further clarification needed).")
            print(f"   Selected: {matches[0]['make']} {matches[0]['model']} {matches[0]['series']}")

def generateClarificationQuestion(question, options):
    print("🤖 AI Response:")
    print(f"   \"{question}\"") 
    structured_options = []
    for key, values in options.items():
        # Clean up the key for display (e.g., "drive_type" -> "Drive Type")
        display_label = key.replace("_", " ").title()
        # Save for later use (e.g., sending to API)
        structured_options.append({
            "field": key,
            "choices": values
        })
        print("") # Empty line

        print("=" * 50)
        print("📦 API Response Format:")
        print(structured_options)
# ==========================================
#   TESTING SECTION
# ==========================================
if __name__ == "__main__":
    # Point to your local folder
    rag = VehicleRAG(db_path="./vehicle_db")

    RIVIAN_R1S_2025 = {
        'make': 'RIVIAN',
        'model': 'R1S',
        'year': 2025,  # Future year
        'style': 'Sport Utility Vehicle',
        'engine': 'ELECTRIC'
    }
    
    query = construct_query(RIVIAN_R1S_2025)
    matches = rag.search(query, n_results=5)
    analyzeMatches(matches)
    
    query = "2022 MERCEDES EQB"
    matches = rag.search(query, n_results=5)
    analyzeMatches(matches)
    