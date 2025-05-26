from pymongo import MongoClient, UpdateOne
import os
from dotenv import load_dotenv



class MongoManager:
    """Handle Mongo database operations"""
    
    # Load environment variables for database connection
    load_dotenv()
    
    def __init__(self):
        # Connect to the database
        self.connection_string = os.getenv('MONGODB_URI')
        self.client = MongoClient(self.connection_string)
        self.database = self.client['steam_db']

    def upsert_app_names(self, app_names: dict):
        """Update the app list in the database with UPSERT logic"""
        collection = self.database['names']
        
        # Prepare bulk update operations
        operations = [
            UpdateOne(
                {'appid': app['appid']}, # Find by appid
                {'$set': {'name': app['name']}}, # Update the name
                upsert=True # Insert if not exists
            )
            for app in app_names['applist']['apps']
        ]
        
        # Perform bulk update
        if operations:
            collection.bulk_write(operations)
        
        print("App names updated successfully")
            
    def upsert_app_details(self, appid: int, app_details: dict):
        """Update a single app detail in the database with UPSERT logic"""
        collection = self.database['details']
        str_appid = str(appid)
            
        # Check that data exists, else it is a test appid
        if str_appid in app_details and 'data' in app_details[str_appid] and app_details[str_appid]['data']:

        
            # Prepare the update operation with upsert logic
            result = collection.update_one(
                {'appid': appid},  # Find by appid, and insert it as int
                {'$set': app_details[str_appid]['data']},  # Update the app details
                upsert=True  # Insert if not exists
            )
            
            if result.upserted_id:  # This means the document was inserted
                pass
                # tqdm.write(f"[INSERTED] appid: {appid}")
            elif result.modified_count > 0:
                pass
                # tqdm.write(f"[UPDATED] appid: {appid}")
            else:
                pass
                # tqdm.write(f"[UNCHANGED] appid: {appid} — Already up-to-date")
            
        else:
            collection = self.database['no_details']
            
            # Increment 'tries' field if exists, otherwise insert with tries=1
            result = collection.update_one(
                {'appid': appid},
                {
                    '$setOnInsert': {'appid': appid},
                    '$inc': {'tries': 1},
                },
                upsert=True
            )
            # tqdm.write(f"[LOGGED] appid: {appid} — Missing or empty data block")
    
    def upsert_app_reviews(self, appid: int, app_reviews: dict):
        """Update a single app review in the database with UPSERT logic"""
        collection = self.database['reviews']
        
        # Prepare the update operation with upsert logic
        result = collection.update_one(
            {'appid': appid},  # Find by appid, and insert it as int
            {'$set': app_reviews},  # Update the app reviews
            upsert=True  # Insert if not exists
        )
        
        if result.upserted_id:  # This means the document was inserted
            pass
            # tqdm.write(f"[INSERTED] appid: {appid}")
        elif result.modified_count > 0:
            pass
            # tqdm.write(f"[UPDATED] appid: {appid}")
        else:
            pass
            # tqdm.write(f"[UNCHANGED] appid: {appid} — Already up-to-date")
            
    def upsert_app_tags(self, appid: int, app_tags: dict):
        """Update a single app tag in the database with UPSERT logic"""
        collection = self.database['tags']
        
        # Prepare the update operation with upsert logic
        result = collection.update_one(
            {'appid': appid},  # Find by appid, and insert it as int
            {'$set': app_tags},  # Update the app tags
            upsert=True  # Insert if not exists
        )
        
        if result.upserted_id:  # This means the document was inserted
            pass
            # tqdm.write(f"[INSERTED] appid: {appid}")
        elif result.modified_count > 0:
            pass
            # tqdm.write(f"[UPDATED] appid: {appid}")
        else:
            pass
            # tqdm.write(f"[UNCHANGED] appid: {appid} — Already up-to-date")