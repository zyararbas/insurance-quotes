import os
from dotenv import load_dotenv, find_dotenv
from enum import Enum
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, PyMongoError
import logging
import pandas as pd

load_dotenv(find_dotenv())

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
class STORAGE_COLLECTIONS(Enum):
        ADDITIONAL_QUOTE_INFO="additional-quote-info"
        INSURANCE_AGENTS="insurance-agents"

class StorageService:
    _instance = None
    
    COLLECTIONS =  {
       
        
    }

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(StorageService, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized'): # Ensure __init__ is called only once for singleton
            self.MONGO_HOST = os.environ.get("MONGO_HOST", "localhost")
            self.MONGO_PORT = int(os.environ.get("MONGO_PORT", 27017))
            self.MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "coveragecompassai")
            self.MONGO_USER = os.environ.get("MONGO_USER", "")
            self.MONGO_PASSWORD = os.environ.get("MONGO_PASSWORD", "")
            self._client = None
            self._db = None
            # self._collection = None
            self._initialized = True
            self.connect()

    def connect(self):
        try:
            self._client = MongoClient(
                self.MONGO_HOST, 
                self.MONGO_PORT,
                username=self.MONGO_USER,
                password=self.MONGO_PASSWORD
                )
            # The ismaster command is cheap and does not require auth.
            self._client.admin.command('ismaster')
            self._db = self._client[self.MONGO_DB_NAME]
            # self._collection = self._db[self.MONGO_COLLECTION_NAME]
            logging.info(f"MongoDB connection established to {self.MONGO_DB_NAME}")
        except ConnectionFailure as e:
            logging.error(f"MongoDB connection error: {e}")
            raise
        except Exception as e:
            logging.error(f"An unexpected error occurred during MongoDB connection: {e}")
            raise

    def close(self):
        if self._client:
            self._client.close()
            logging.info("MongoDB connection closed.")

    def find(self, query: dict, collection_name: str):
        if self._db is None:
            raise ConnectionError("MongoDB database not initialized. Call connect() first.")
        try:
            collection = self._db[collection_name]
            logging.info(f"Finding documents in collection '{collection_name}' with query: {query}")
            return list(collection.find(query))
        except PyMongoError as e:
            logging.error(f"Error finding documents in collection {collection_name}: {e}")
            return []

    def insert_one(self, document: dict, collection_name: str):
        if self._db is None:
            raise ConnectionError("MongoDB database not initialized. Call connect() first.")
        try:
            collection = self._db[collection_name]
            if "_id" in document:
                # Perform an "upsert" if an _id is provided.
                # This will insert the document if it doesn't exist,
                # or replace it if it does.
                filter_query = {"_id": document["_id"]}
                result = collection.replace_one(filter_query, document, upsert=True)

                if result.upserted_id is not None:
                    logging.info(f"Document inserted (upserted) into collection {collection_name} with ID: {result.upserted_id}")
                    return result.upserted_id
                else:
                    logging.info(f"Document with ID {document['_id']} updated or matched in collection {collection_name}.")
                    return document['_id']
            else:
                # Original behavior: insert a new document.
                result = collection.insert_one(document)
                logging.info(f"Document inserted into collection {collection_name} with ID: {result.inserted_id}")
                return result.inserted_id
        except PyMongoError as e:
            logging.error(f"Error inserting/upserting document in collection {collection_name}: {e}")
            return None

    def update_one(self, query: dict, update: dict, collection_name: str):
        if self._db is None:
            raise ConnectionError("MongoDB database not initialized. Call connect() first.")
        try:
            collection = self._db[collection_name]
            result = collection.update_one(query, {"$set": update})
            logging.info(f"Documents matched in collection {collection_name}: {result.matched_count}, modified: {result.modified_count}")
            return result.modified_count
        except PyMongoError as e:
            logging.error(f"Error updating document in collection {collection_name}: {e}")
            return 0

    def delete_one(self, query: dict, collection_name: str):
        if self._db is None:
            raise ConnectionError("MongoDB database not initialized. Call connect() first.")
        try:
            collection = self._db[collection_name]
            result = collection.delete_one(query)
            logging.info(f"Documents deleted from collection {collection_name}: {result.deleted_count}")
            return result.deleted_count
        except PyMongoError as e:
            logging.error(f"Error deleting document from collection {collection_name}: {e}")
            return 0 

    def get_collection_as_dataframe(self, collection_name: str) -> pd.DataFrame:
        """
        Fetches all documents from a specified collection and returns them as a pandas DataFrame.
        
        Args:
            collection_name: The name of the collection to fetch.  
        
        Returns:
            A pandas DataFrame containing the collection data.
        """
        if self._db is None:
            raise ConnectionError("MongoDB database not initialized. Call connect() first.")
        try:
            logging.info(f"Fetching all documents from collection: '{collection_name}'")
            # Pass an empty query to find all documents
            documents = self.find({}, collection_name)
            if not documents:
                logging.warning(f"No documents found in collection '{collection_name}'.")
                return pd.DataFrame()
            
            df = pd.DataFrame(documents)
            return df
        except PyMongoError as e:
            logging.error(f"Error fetching collection {collection_name} as DataFrame: {e}")
            return pd.DataFrame() # Return empty dataframe on error