import os
import logging
from pymongo import MongoClient, UpdateOne
from datetime import datetime


logger = logging.getLogger("MongoManager")

class MongoManager:
    """Handle MongoDB operations"""
    
    def __init__(self):
        self.connection_string = os.getenv('MONGODB_CONNECTION_STRING')
        self.client = MongoClient(self.connection_string)
        self.database = self.client['steam_db']

    def upsert_app_names(self, app_names: dict):
        """Upsert of app names into 'names' collection"""
        collection = self.database['names']
        operations = [
            UpdateOne(
                {'appid': app['appid']},
                {   
                    '$set': {
                        'name': app['name'],
                        'updated_at': datetime.now()
                    }
                },
                upsert=True
            )
            for app in app_names['applist']['apps']
        ]
        if operations:
            result = collection.bulk_write(operations)
            logger.info(
                f"Upserted app names - Total: {len(operations)}, "
                f"Unchanged: {result.matched_count}, "
                f"Updated: {result.modified_count}, "
                f"Inserted: {result.upserted_count}"
            )
        else:
            logger.warning("No app names to upsert")

    def upsert_app_details(self, appid: int, app_details: dict):
        """Upsert of app details into 'details' or 'no_details'"""
        collection = self.database['details']
        str_appid = str(appid)

        if str_appid in app_details and 'data' in app_details[str_appid] and app_details[str_appid]['data']:
            result = collection.update_one(
                {'appid': appid},
                {'$set': app_details[str_appid]['data']},
                upsert=True
            )
            if result.upserted_id:
                logger.info(f"[INSERTED] Details for AppID {appid}")
            elif result.modified_count > 0:
                logger.info(f"[UPDATED] Details for AppID {appid}")
            else:
                logger.info(f"[UNCHANGED] Details for AppID {appid}")
        else:
            # Log missing details
            no_details_col = self.database['no_details']
            result = no_details_col.update_one(
                {'appid': appid},
                {
                    '$setOnInsert': {'appid': appid},
                    '$inc': {'tries': 1},
                },
                upsert=True
            )
            if result.upserted_id:
                logger.info(f"[INSERTED] No Details for AppID {appid}")
            elif result.modified_count > 0:
                logger.info(f"[UPDATED] No Details for AppID {appid}")
            else:
                logger.info(f"[UNCHANGED] No Details for AppID {appid}")

    def upsert_app_tags(self, appid: int, app_tags: dict):
        """Upsert of app tags into 'tags' collection"""
        collection = self.database['tags']
        result = collection.update_one(
            {'appid': appid},
            {'$set': app_tags},
            upsert=True
        )
        if result.upserted_id:
            logger.info(f"[INSERTED] Tags for AppID {appid}")
        elif result.modified_count > 0:
            logger.info(f"[UPDATED] Tags for AppID {appid}")
        else:
            logger.info(f"[UNCHANGED] Tags for AppID {appid}")

    def upsert_app_reviews(self, appid: int, app_reviews: dict):
        """Upsert of app reviews into 'reviews' collection"""
        collection = self.database['reviews']
        result = collection.update_one(
            {'appid': appid},
            {'$set': app_reviews},
            upsert=True
        )
        if result.upserted_id:
            logger.info(f"[INSERTED] Reviews for AppID {appid}")
        elif result.modified_count > 0:
            logger.info(f"[UPDATED] Reviews for AppID {appid}")
        else:
            logger.info(f"[UNCHANGED] Reviews for AppID {appid}")