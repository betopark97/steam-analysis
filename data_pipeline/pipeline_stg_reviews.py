# Managers
from data_pipeline.managers.mongo_manager import MongoManager
from data_pipeline.managers.postgres_manager import PostgresManager
from data_pipeline.utils.utils_pipeline import (
    to_pg_array_str_safe,
    normalize_strings_df,
    parse_steam_review_html_df,
    parse_steam_review_score_df,
)
from data_pipeline.utils.utils_logging import setup_logging

# Utils
import json
from bson import ObjectId
from more_itertools import chunked

# Data Science
import polars as pl

import logging
import warnings
from bs4 import MarkupResemblesLocatorWarning

warnings.simplefilter("ignore", category=MarkupResemblesLocatorWarning)

setup_logging()
logger = logging.getLogger(__name__)


def get_stg_reviews_filtered_appids(mongo_manager: MongoManager) -> list:
    """Get the list of filtered IDs for the 'reviews' collection in MongoDB."""
    # Filter documents
    query = {
        # Filter type in the list
        'type': {'$in': ['game', 'dlc', 'demo', 'series', 'episode', 'music', 'mod']},
        # Filter only already published apps
        'release_date.coming_soon': False
    }

    # Read only necessary columns
    projection = {
        "_id": 0,
        'appid': 1,
    }

    # Fetch filtered appids
    cursor = mongo_manager.database.details.find(query, projection)
    list_cursor = [document['appid'] for document in cursor]

    # Fetch reviews ids for the filtered appids
    cursor_reviews = mongo_manager.database.reviews.find(
        {"appid": {"$in": list_cursor}},
        {"_id": 0, "appid": 1}
    )
    list_cursor = [document['appid'] for document in cursor_reviews]
    return list_cursor

def process_stg_reviews_filtered(mongo_manager: MongoManager, postgres_manager: PostgresManager, filtered_appids: list) -> None:
    """Process the filtered reviews from MongoDB and insert into Postgres."""
    logger.info(f"Starting processing of {len(filtered_appids)} filtered reviews...")
    
    # Define the batch size for processing
    batch_size = 1000
    logger.info(f"Processing in batches of {batch_size}...")
    # Read only necessary columns
    projection = {
        "_id": 0, "appid": 1, "html": 1, "review_score": 1
    }

    for batch_index, appids in enumerate(chunked(filtered_appids, batch_size), start=1):
        logger.info(f"Processing batch {batch_index} with {len(appids)} appids")
        # Fetch documents
        cursor = mongo_manager.database.reviews.find({'appid': {'$in': appids}}, projection)
        list_cursor = list(cursor)
        
        df = pl.LazyFrame(list_cursor, strict=False, infer_schema_length=len(list_cursor))
        
        df_final = (
            df
            .pipe(normalize_strings_df, cols=['html', 'review_score'])
            .pipe(parse_steam_review_html_df, col='html')
            .pipe(parse_steam_review_score_df, col='review_score')
        )

        df_dicts = df_final.collect().to_dicts()

        for row in df_dicts:
            for key, value in row.items():

                # Case 1: Convert [] to None
                if isinstance(value, list):
                    if value == []:
                        row[key] = None

                    elif all(isinstance(v, str) for v in value):
                        row[key] = to_pg_array_str_safe(value)

                    else:
                        row[key] = json.dumps(value, ensure_ascii=False)

                # Case 2: Convert {"a": None, "b": None} to None
                elif isinstance(value, dict):
                    if all(v is None for v in value.values()):
                        row[key] = None
                    else:
                        row[key] = json.dumps(value, ensure_ascii=False)
        logger.info("Converted DataFrame to match Postgres insertion expectations...")

        # Convert back to Polars DataFrame
        df_final_cleaned = pl.DataFrame(df_dicts, strict=False, infer_schema_length=len(list_cursor))
        
        # Insert into Postgres
        postgres_manager.upsert_app_reviews(df_final_cleaned)
        logger.info(f"Inserted {len(df_final_cleaned)} rows into Postgres successfully...")
