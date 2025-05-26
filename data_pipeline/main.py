# System
import os
from dotenv import load_dotenv

# Service Managers
from managers.steam_api_manager import SteamAPIManager
from managers.mongo_manager import MongoManager
from managers.postgres_manager import PostgresManager

# Data Pipeline
from pipeline import extract, transform, load


def process_app_names(
    steam_api_manager: SteamAPIManager, 
    mongo_manager: MongoManager
) -> None:
    """Process app names from Steam API and load to MongoDB"""
    app_names = extract.extract_app_names(steam_api_manager)
    load.load_app_names_to_mongo(mongo_manager, app_names)
    


if __name__ == "__main__":
    # Load environment variables
    load_dotenv()
    
    # Instantiate managers
    steam_api_manager = SteamAPIManager()
    mongo_manager = MongoManager()
    postgres_manager = PostgresManager()
    
    # Run data pipeline
    
    
    extract.extract_app_names(steam_api_manager)
    extract.extract_app_details(steam_api_manager, app_id)
    extract.extract_app_tags(steam_api_manager, app_id)
    extract.extract_app_reviews(steam_api_manager, app_id)
    
    