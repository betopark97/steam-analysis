# Managers
from data_pipeline.managers.mongo_manager import MongoManager
from data_pipeline.managers.postgres_manager import PostgresManager
from data_pipeline.utils.utils_pipeline import (
    convert_mixed_columns_to_string,
    to_pg_array_str_safe,
    remove_html_tags_df,
    normalize_strings_df,
    strip_list_strings_df,
    remove_plus_sign_df,
    convert_age_to_int_df,
    filter_age_df,
    is_diff_texts
)
# Utils
import json
from bson import ObjectId
from more_itertools import chunked
# Data Science
import pandas as pd
import polars as pl

import logging
from data_pipeline.utils.utils_logging import setup_logging
import warnings
from bs4 import MarkupResemblesLocatorWarning

warnings.simplefilter("ignore", category=MarkupResemblesLocatorWarning)

setup_logging()
logger = logging.getLogger(__name__)


def get_stg_details_filtered_ids(mongo_manager: MongoManager) -> list:
    """Get the list of filtered IDs for the 'details' collection in MongoDB."""
    # Filter documents
    query = {
        # Filter type in the list
        'type': {'$in': ['game', 'dlc', 'demo', 'series', 'episode', 'music', 'mod']},
        # Filter only already published apps
        'release_date.coming_soon': False
    }

    # Read only necessary columns
    projection = {
        '_id': 1,
    }

    # Fetch documents
    cursor = mongo_manager.database.details.find(query, projection)
    list_cursor = list(cursor)

    # Print count of documents
    print(f"Number of documents: {len(list_cursor)}")
    return [str(document['_id']) for document in list_cursor]

def process_stg_details_filtered(mongo_manager: MongoManager, postgres_manager: PostgresManager, filtered_str_ids: list) -> None:
    """Process the filtered details from MongoDB and insert into Postgres."""
    logger.info(f"Starting processing of {len(filtered_str_ids)} filtered details...")
    # Convert string IDs to ObjectId
    filtered_ids = [ObjectId(id_str) for id_str in filtered_str_ids]

    # Define the batch size for processing
    batch_size = 1000
    logger.info(f"Processing in batches of {batch_size}...")
    # Read only necessary columns
    projection = {
        '_id': 0,
        # Key
        'appid': 1, 'name': 1, 'fullgame.appid': 1,
        # Description
        'about_the_game': 1, 'detailed_description': 1, 'short_description': 1,
        # Images
        'header_image': 1, 'background_raw': 1, 'screenshots': 1, 'movies': 1,
        # Price
        'is_free': 1, 'price_overview.currency': 1, 'price_overview.initial': 1,
        # Meta
        'type': 1, 'categories': 1, 'genres': 1, 'supported_languages': 1, 'controller_support': 1,
        # Date
        'release_date.date': 1,
        # Contract
        'developers': 1, 'publishers': 1, 'content_descriptors.notes': 1,
        # Progress
        'achievements.highlighted': 1,
        # Computer Specs
        'platforms': 1, 
        'pc_requirements.minimum': 1, 'pc_requirements.recommended': 1,
        'mac_requirements.minimum': 1, 'mac_requirements.recommended': 1,
        'linux_requirements.minimum': 1, 'linux_requirements.recommended': 1,
        # Required Age
        'required_age': 1,
        'ratings.dejus.required_age': 1, 'ratings.steam_germany.required_age': 1, 'ratings.csrr.required_age': 1,
        'ratings.esrb.required_age': 1, 'ratings.pegi.required_age': 1, 'ratings.usk.required_age': 1,
        'ratings.oflc.required_age': 1, 'ratings.nzoflc.required_age': 1, 'ratings.kgrb.required_age': 1,
        'ratings.mda.required_age': 1, 'ratings.fpb.required_age': 1, 'ratings.crl.required_age': 1,
        'ratings.bbfc.required_age': 1, 'ratings.cero.required_age': 1, 'ratings.agcom.required_age': 1,
        'ratings.cadpa.required_age': 1, 'ratings.video.required_age': 1,
    }

    for batch_index, ids in enumerate(chunked(filtered_ids, batch_size), start=1):
        logger.info(f"Processing batch {batch_index} with {len(ids)} IDs")
        # Fetch documents
        cursor = mongo_manager.database.details.find({'_id': {'$in': ids}}, projection)
        list_cursor = list(cursor)

        # Convert to DataFrame
        df = pd.json_normalize(list_cursor)
        # logger.info("JSON normalized and loaded dataframe...")
        
        # Convert mixed columns to string
        df_cleaned = convert_mixed_columns_to_string(df)
        # logger.info("Converted mixed columns to string...")
        
        # Convert to Polars LazyFrame
        df_pl = pl.LazyFrame(
            df_cleaned,
            strict=False,
            infer_schema_length=batch_size
        )
        # logger.info("Converted DataFrame to Polars LazyFrame...")
        
        # Drop unnecessary columns early
        columns_to_drop = ['pc_requirements', 'mac_requirements', 'linux_requirements', 'ratings']
        columns_to_drop_in_df = [col for col in columns_to_drop if col in df_pl.collect_schema().names()]
        df_pl = df_pl.drop(columns_to_drop_in_df)
        # logger.info(f"Dropped unnecessary columns: {columns_to_drop_in_df}")

        # Basic filters with a single condition is done in MongoDB but multiple conditions will be done in polars
        columns_except_appid = [col for col in df_pl.collect_schema().names() if col != 'appid']
        df_filtered = (
            df_pl
            .filter(
            # Filter out type != game but have no fullgame.appid
                ~(
                    (pl.col('type') != 'game') &
                    (pl.col('fullgame.appid').is_null())
                )
            )
            # Filter out duplicate data (when different appid but rest of the columns are same)
            .sort("appid")
            .unique(subset=columns_except_appid, keep='first')
        )
        # logger.info("Filtered DataFrame...")
        
        # Type optimization
        df_type_optimized = (
            df_filtered
            .with_columns(
                # Convert List[Struct{}] to List[str]
                pl.col('categories').list.eval(pl.element().struct.field('description')),
                pl.col('genres').list.eval(pl.element().struct.field('description')),
                pl.col('screenshots').list.eval(pl.element().struct.field('path_full')),
                pl.col('movies').list.eval(pl.element().struct.field('mp4').struct.field('max')),
                # Convert String to Int
                pl.col('fullgame.appid').cast(pl.Int64),
                # Convert String to Boolean
                pl
                .when(pl.col('controller_support') == 'full')
                .then(pl.lit(True))
                .when(pl.col('controller_support').is_null())
                .then(pl.lit(False))
                .otherwise(pl.lit(None))
                .alias('controller_support')
            )
        )
        # logger.info("Optimized DataFrame types...")
        
        # Data Cleaning
        cols_str = [
            col 
            for col, dtype in df_type_optimized.collect_schema().items() 
            if dtype == pl.String
        ]
        cols_list_string = [
            col 
            for col, dtype in df_type_optimized.collect_schema().items()
            if isinstance(dtype, pl.List) and dtype.inner == pl.String
        ]
        cols_age = [
            col 
            for col in df_type_optimized.collect_schema().names() 
            if 'required_age' in col
        ]
        df_cleaned = (
            df_type_optimized
            # Normalize all strings
            .pipe(normalize_strings_df, cols=cols_str)
            .pipe(remove_html_tags_df, cols=cols_str)
            
            # Strip all list strings
            .pipe(strip_list_strings_df, cols=cols_list_string)
            
            # Clean age
            .pipe(remove_plus_sign_df, cols=cols_age)
            .pipe(convert_age_to_int_df, cols=cols_age)
            .pipe(filter_age_df, cols=cols_age)
        )
        # logger.info("Cleaned DataFrame...")
        
        # Data Wrangling
        # Merge required_ages
        df_merged_ages = (
            df_cleaned
            # Get the highest required age value across all required_age columns
            .with_columns(
                pl.max_horizontal(cols_age).alias('required_age_cleaned'),
            )
            # Drop all other required_age columns
            .drop(cols_age)
            # Rename the required_age_cleaned column
            .rename({
                'required_age_cleaned': 'required_age'
            })
        )
        
        # Merge descriptions
        df_merged_descriptions = (
            df_merged_ages
            # Compute if descriptions are different
            .with_columns(
                pl.struct(["about_the_game", "detailed_description"])
                .map_elements(
                    lambda x: is_diff_texts(x["about_the_game"], x["detailed_description"]), 
                    return_dtype=pl.Boolean
                )
                .alias("desc_has_diff")
            )

            # Merge descriptions based on logic
            .with_columns(
                # Case 1: if both are empty
                pl.when((pl.col("about_the_game") == "") & (pl.col("detailed_description") == ""))
                # then empty string
                .then(pl.lit(None))  

                # Case 2: if about_the_game and not detailed_description
                .when((pl.col("about_the_game") != "") & (pl.col("detailed_description") == ""))
                # then about_the_game
                .then(  
                    pl.concat_str([
                        pl.lit("[about_the_game]\n"),
                        pl.col("about_the_game")
                    ], separator="")
                )
                
                # Case 3: if not about_the_game and detailed_description
                .when((pl.col("about_the_game") == "") & (pl.col("detailed_description") != ""))
                # then detailed_description
                .then(
                    pl.concat_str([
                        pl.lit("[detailed_description]\n"),
                        pl.col("detailed_description")
                    ], separator="")
                )

                # Case 4: if both but different
                .when(pl.col("desc_has_diff"))
                # then about_the_game + detailed_description
                .then(  
                    pl.concat_str([
                        pl.lit("[about_the_game]\n"),
                        pl.col("about_the_game"),
                        pl.lit("\n\n[detailed_description]\n"),
                        pl.col("detailed_description")
                    ], separator="")
                )

                # Case 5: if both but same
                .otherwise(  # Case 5: both present but identical or not different enough
                    pl.concat_str([
                        pl.lit("[detailed_description]\n"),
                        pl.col("detailed_description")
                    ], separator="")
                )
                .alias("detailed_description"),
                
                # Short description
                pl.when(
                    pl.col('short_description') == ''
                )
                .then(
                    pl.lit(None)
                )
                .otherwise(
                    pl.col('short_description')
                )
                .alias('short_description')
            )

            # Add boolean flag for non-empty final description
            .with_columns(
                ~(
                    pl.concat_str([
                        pl.col("detailed_description").fill_null(''),
                        pl.col("short_description").fill_null(''),
                        pl.col("content_descriptors.notes").fill_null('')
                    ])
                    .str.strip_chars()
                    .str.len_chars() > 0
                )
                .alias("enrich.is_description")
            )

            # Drop temp columns
            .drop("desc_has_diff", "about_the_game")
        )
        
        # Clean dates
        df_cleaned_dates = (
            df_merged_descriptions
            .with_columns(
                pl.coalesce([
                    pl.col("release_date.date").str.to_titlecase().str.strptime(pl.Date, "%d %b %Y", strict=False), # 1 Jan 2025
                    pl.col("release_date.date").str.to_titlecase().str.strptime(pl.Date, "%d. %b. %Y", strict=False), # 1. Jan. 2025
                    pl.col("release_date.date").str.strptime(pl.Date, "%d %b, %Y", strict=False), # 1 Jan, 2025
                    pl.col("release_date.date").str.strptime(pl.Date, "%b %d, %Y", strict=False), # Jan 1, 2025
                ]).alias("release_date.date")
            )
            .with_columns(
                ~(pl.col("release_date.date").is_not_null())
                .alias("enrich.is_date")
            )
        )
        
        # Clean computer specs
        df_cleaned_computer_specs = (
            df_cleaned_dates
            .with_columns(
                # Windows Minimum
                pl.when(
                    ~(pl.col('platforms.windows')) |
                    (pl.col('pc_requirements.minimum')
                    .str.strip_chars()
                    .str.strip_chars(":")  # remove trailing colons just in case
                    .str.to_lowercase() == "minimum")
                )
                .then(pl.lit(None))
                .otherwise(pl.col('pc_requirements.minimum'))
                .alias('pc_requirements.minimum'),

                # Windows Recommended
                pl.when(
                    ~(pl.col('platforms.windows')) |
                    (pl.col('pc_requirements.recommended')
                    .str.strip_chars()
                    .str.strip_chars(":")
                    .str.to_lowercase() == "recommended")
                )
                .then(pl.lit(None))
                .otherwise(pl.col('pc_requirements.recommended'))
                .alias('pc_requirements.recommended'),

                # Mac Minimum
                pl.when(
                    ~(pl.col('platforms.mac')) |
                    (pl.col('mac_requirements.minimum')
                    .str.strip_chars()
                    .str.strip_chars(":")
                    .str.to_lowercase() == "minimum")
                )
                .then(pl.lit(None))
                .otherwise(pl.col('mac_requirements.minimum'))
                .alias('mac_requirements.minimum'),

                # Mac Recommended
                pl.when(
                    ~(pl.col('platforms.mac')) |
                    (pl.col('mac_requirements.recommended')
                    .str.strip_chars()
                    .str.strip_chars(":")
                    .str.to_lowercase() == "recommended")
                )
                .then(pl.lit(None))
                .otherwise(pl.col('mac_requirements.recommended'))
                .alias('mac_requirements.recommended'),

                # Linux Minimum
                pl.when(
                    ~(pl.col('platforms.linux')) |
                    (pl.col('linux_requirements.minimum')
                    .str.strip_chars()
                    .str.strip_chars(":")
                    .str.to_lowercase() == "minimum")
                )
                .then(pl.lit(None))
                .otherwise(pl.col('linux_requirements.minimum'))
                .alias('linux_requirements.minimum'),

                # Linux Recommended
                pl.when(
                    ~(pl.col('platforms.linux')) |
                    (pl.col('linux_requirements.recommended')
                    .str.strip_chars()
                    .str.strip_chars(":")
                    .str.to_lowercase() == "recommended")
                )
                .then(pl.lit(None))
                .otherwise(pl.col('linux_requirements.recommended'))
                .alias('linux_requirements.recommended'),
            )
            .with_columns(
                # Enrich Windows
                pl.when(
                    (pl.col('platforms.windows')) & 
                    (
                        (pl.col('pc_requirements.minimum').is_null()) &
                        (pl.col('pc_requirements.recommended').is_null())
                    )
                )
                .then(pl.lit(True))
                .otherwise(pl.lit(False))
                .alias('enrich.is_windows'),

                # Enrich Mac
                pl.when(
                    (pl.col('platforms.mac')) & 
                    (
                        (pl.col('mac_requirements.minimum').is_null()) &
                        (pl.col('mac_requirements.recommended').is_null())
                    )
                )
                .then(pl.lit(True))
                .otherwise(pl.lit(False))
                .alias('enrich.is_mac'),

                # Enrich Linux
                pl.when(
                    (pl.col('platforms.linux')) & 
                    (
                        (pl.col('linux_requirements.minimum').is_null()) &
                        (pl.col('linux_requirements.recommended').is_null())
                    )
                )
                .then(pl.lit(True))
                .otherwise(pl.lit(False))
                .alias('enrich.is_linux')
            )
        )        
    
        # Clean price
        df_cleaned_price = (
            df_cleaned_computer_specs
            .with_columns(
                # Divide price by 100
                (pl.col('price_overview.initial') / 100).alias('price_overview.initial'),
                # Enrich Price
                pl.when(
                    ~(pl.col('is_free')) & 
                    (
                        (pl.col('price_overview.currency').is_null()) |
                        (pl.col('price_overview.initial').is_null())
                    )
                )
                .then(pl.lit(True))
                .otherwise(pl.lit(False))
                .alias('enrich.is_price')
            )
        )
        # logger.info("Wrangled DataFrame...")

        # Column reordering & renaming
        column_schema = [
            'appid', 'name', 'fullgame.appid',
            'enrich.is_description', 'detailed_description', 'short_description',
            'header_image', 'background_raw', 'screenshots', 'movies',
            'enrich.is_price', 'is_free', 'price_overview.currency', 'price_overview.initial',
            'type', 'categories', 'genres', 'supported_languages', 'controller_support',
            'enrich.is_date', 'release_date.date',
            'required_age',
            'developers', 'publishers', 'content_descriptors.notes',
            'achievements.highlighted',
            'enrich.is_windows', 'platforms.windows', 'pc_requirements.minimum', 'pc_requirements.recommended',
            'enrich.is_mac', 'platforms.mac', 'mac_requirements.minimum', 'mac_requirements.recommended',
            'enrich.is_linux', 'platforms.linux', 'linux_requirements.minimum', 'linux_requirements.recommended'
        ]

        df_cleaned_schema = df_cleaned_price.clone()
        cols_not_in_schema = [col for col in df_cleaned_price.collect_schema().names() if col not in column_schema]
        for col in cols_not_in_schema:
            df_cleaned_schema = df_cleaned_schema.with_columns(pl.lit(None).alias(col))
        # logger.info(f"Added missing columns: {cols_not_in_schema}")

        df_final = (
            df_cleaned_schema
            # Reorder and Rename columns
            .select(
                # Keys
                'appid', 'name', 
                pl.col('fullgame.appid').alias('game_appid'),
                # Game descriptions
                pl.col('enrich.is_description').alias('enrich_is_description'), 
                pl.col('detailed_description').alias('long_description'),
                'short_description',
                # Images / Movies
                pl.col('header_image').alias('image_header'),
                pl.col('background_raw').alias('image_background'),
                pl.col('screenshots').alias('image_screenshots'),
                pl.col('movies').alias('image_movies'),
                # Price
                pl.col('enrich.is_price').alias('enrich_is_price'),
                'is_free', 
                pl.col('price_overview.currency').alias('currency'),
                pl.col('price_overview.initial').alias('price'),
                # Meta
                'type', 'categories', 'genres', 'supported_languages', 
                pl.col('controller_support').alias('is_controller_support'),
                # Date
                pl.col('enrich.is_date').alias('enrich_is_date'),
                pl.col('release_date.date').alias('release_date'),
                # Age
                'required_age',
                # Contract
                'developers', 'publishers', 
                pl.col('content_descriptors.notes').alias('content_descriptors_notes'),
                # Progress
                pl.col('achievements.highlighted').alias('achievements'),
                # Computer Specs
                pl.col('enrich.is_windows').alias('enrich_is_windows'),
                pl.col('platforms.windows').alias('is_windows'), 
                pl.col('pc_requirements.minimum').alias('windows_requirements_minimum'),
                pl.col('pc_requirements.recommended').alias('windows_requirements_recommended'),
                pl.col('enrich.is_mac').alias('enrich_is_mac'),
                pl.col('platforms.mac').alias('is_mac'),
                pl.col('mac_requirements.minimum').alias('mac_requirements_minimum'),
                pl.col('mac_requirements.recommended').alias('mac_requirements_recommended'),
                pl.col('enrich.is_linux').alias('enrich_is_linux'),
                pl.col('platforms.linux').alias('is_linux'),
                pl.col('linux_requirements.minimum').alias('linux_requirements_minimum'),
                pl.col('linux_requirements.recommended').alias('linux_requirements_recommended'),
            )
        )
        # logger.info("Final DataFrame prepared with correct schema (rename & reorder columns)...")
        
        # Clean for Postgres insertion
        # Convert to Python-native list of dicts
        df_dicts = df_final.collect().to_dicts()

        for row in df_dicts:
            for key, value in row.items():
                if isinstance(value, list) and all(isinstance(v, str) for v in value):
                    row[key] = to_pg_array_str_safe(value)
                elif isinstance(value, (list, dict)):
                    row[key] = json.dumps(value, ensure_ascii=False)
        # logger.info("Converted DataFrame to match Postgres insertion expectations...")

        # Convert back to Polars DataFrame
        df_final_cleaned = pl.DataFrame(df_dicts, infer_schema_length=batch_size)

        # Insert into Postgres
        postgres_manager.upsert_app_details(df_final_cleaned)
        logger.info(f"Inserted {len(df_final_cleaned)} rows into Postgres successfully...")

if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    load_dotenv()
    mongo_manager = MongoManager(connection_string=os.getenv("MONGODB_URI"))
    postgres_manager = PostgresManager()
    postgres_manager.db_params['host'] = 'localhost'
    partition_ids = get_stg_details_filtered_ids(mongo_manager)
    process_stg_details_filtered(mongo_manager=mongo_manager, postgres_manager=postgres_manager, filtered_str_ids=partition_ids)

