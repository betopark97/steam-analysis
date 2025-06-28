# Managers
from data_pipeline.managers.mongo_manager import MongoManager
from data_pipeline.managers.postgres_manager import PostgresManager
from data_pipeline.utils.utils_pipeline import (
    to_pg_array_str_safe,
)
# Utils
import json
from bson import ObjectId
from more_itertools import chunked
# Data Science
import polars as pl

from pathlib import Path
import sys
import logging
from logging.handlers import RotatingFileHandler

import warnings
from bs4 import MarkupResemblesLocatorWarning

warnings.simplefilter("ignore", category=MarkupResemblesLocatorWarning)

def setup_logging():
    # Resolve the directory of this script file
    log_dir = Path(__file__).resolve().parent
    log_path = log_dir / "app.log"
    
    # Clear existing handlers
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # Set up log file rotation: max 200MB, keep 2 backup files
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=200 * 1024 * 1024,  
        backupCount=2,              
        encoding='utf-8'
    )

    stream_handler = logging.StreamHandler(sys.stdout)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[stream_handler, file_handler],
    )

    # Module-specific log levels
    logging.getLogger("MongoManager").setLevel(logging.INFO)
    logging.getLogger("SteamAPIManager").setLevel(logging.INFO)
    

setup_logging()
logger = logging.getLogger(__name__)


def get_stg_tags_filtered_ids(mongo_manager: MongoManager) -> list:
    """Get the list of filtered IDs for the 'details' collection in MongoDB."""
    # Fetch game appids
    cursor_details = mongo_manager.database.details.find(
        {"type": "game", "release_date.coming_soon": False},
        {"_id": 0, "appid": 1}
    )
    game_appids = [doc['appid'] for doc in cursor_details]

    
    cursor_tags = mongo_manager.database.tags.find(
        {"appid": {"$in": game_appids}},
        {"_id": 1}
    )
    list_cursor = list(cursor_tags)

    # Print count of documents
    print(f"Number of documents: {len(list_cursor)}")
    return [str(document['_id']) for document in list_cursor]

def process_stg_tags_filtered(mongo_manager: MongoManager, postgres_manager: PostgresManager, filtered_str_ids: list) -> None:
    """Process the filtered tags from MongoDB and insert into Postgres."""
    logger.info(f"Starting processing of {len(filtered_str_ids)} filtered tags...")
    # Convert string IDs to ObjectId
    filtered_ids = [ObjectId(id_str) for id_str in filtered_str_ids]

    # Define the batch size for processing
    batch_size = 2000
    logger.info(f"Processing in batches of {batch_size}...")
    # Read only necessary columns
    projection = {
        "_id": 0, "appid": 1, "tags": 1
    }

    for batch_index, ids in enumerate(chunked(filtered_ids, batch_size), start=1):
        logger.info(f"Processing batch {batch_index} with {len(ids)} IDs")
        # Fetch documents
        cursor = mongo_manager.database.tags.find({'_id': {'$in': ids}}, projection)
        list_cursor = list(cursor)

        for document in list_cursor:
            if isinstance(document.get('tags'), dict):
                document['tags'] = list(document['tags'].keys())

        df = pl.LazyFrame(list_cursor, strict=False, infer_schema_length=len(list_cursor))
        df_filtered = df.filter(pl.col('tags').list.len() > 0)

        df_dicts = df_filtered.collect().to_dicts()

        for row in df_dicts:
            for key, value in row.items():
                if isinstance(value, list) and all(isinstance(v, str) for v in value):
                    row[key] = to_pg_array_str_safe(value)
                elif isinstance(value, (list, dict)):
                    row[key] = json.dumps(value, ensure_ascii=False)
        # logger.info("Converted DataFrame to match Postgres insertion expectations...")

        # Convert back to Polars DataFrame
        df_final_cleaned = pl.DataFrame(df_dicts, infer_schema_length=len(list_cursor))
        
        # Insert into Postgres
        postgres_manager.upsert_app_tags(df_final_cleaned)
        logger.info(f"Inserted {len(df_final_cleaned)} rows into Postgres successfully...")
