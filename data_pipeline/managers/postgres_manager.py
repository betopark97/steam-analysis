import json
import os
from pathlib import Path
import psycopg2
from dotenv import load_dotenv
import polars as pl
from io import StringIO


class PostgresManager:
    """Handle PostgreSQL database operations"""
    
    # Load environment variables for database connection
    load_dotenv()
    
    def __init__(self):
        # Connect to the database
        self.db_params = {
            'host': os.getenv('DB_HOST'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'database': os.getenv('DB_NAME'),
            'port': os.getenv('DB_PORT')
        }

    def upsert_app_details(self, df_app_details: pl.DataFrame):
        """Update the app details in the database with UPSERT logic"""
        conn = psycopg2.connect(**self.db_params)
        cur = conn.cursor()
        
        try:
            # Create temporary table
            cur.execute("""
                CREATE TEMP TABLE temp_app_details (
                    appid INTEGER PRIMARY KEY,
                    name VARCHAR(255),
                    game_appid INTEGER,
                    enrich_is_description BOOLEAN,
                    long_description TEXT,
                    short_description TEXT,
                    image_header TEXT,
                    image_background TEXT,
                    image_screenshots TEXT [],
                    image_movies TEXT [],
                    enrich_is_price BOOLEAN,
                    is_free BOOLEAN,
                    currency CHAR(3),
                    price NUMERIC(10, 2),
                    type app_type,
                    categories TEXT [],
                    genres TEXT [],
                    supported_languages TEXT,
                    controller_support BOOLEAN,
                    enrich_is_date BOOLEAN,
                    release_date DATE,
                    required_age SMALLINT,
                    developers TEXT [],
                    publishers TEXT [],
                    content_descriptors_notes TEXT,
                    achievements JSONB,
                    enrich_is_windows BOOLEAN,
                    is_windows BOOLEAN,
                    windows_requirements_minimum TEXT,
                    windows_requirements_recommended TEXT,
                    enrich_is_mac BOOLEAN,
                    is_mac BOOLEAN,
                    mac_requirements_minimum TEXT,
                    mac_requirements_recommended TEXT,
                    enrich_is_linux BOOLEAN,
                    is_linux BOOLEAN,
                    linux_requirements_minimum TEXT,
                    linux_requirements_recommended TEXT
                ) ON COMMIT DROP;
            """)
            
            # Copy data to temp table
            buffer = StringIO()
            df_app_details.write_csv(buffer, include_header=False)
            buffer.seek(0)
            
            cur.copy_expert(
                """COPY temp_app_details (
                    appid, name, game_appid, 
                    enrich_is_description, long_description, short_description,
                    image_header, image_background, image_screenshots, image_movies, 
                    enrich_is_price, is_free, currency, price, 
                    type, categories, genres, supported_languages, controller_support,
                    enrich_is_date, release_date, 
                    required_age, 
                    developers, publishers, content_descriptors_notes,
                    achievements, 
                    enrich_is_windows, is_windows, windows_requirements_minimum, windows_requirements_recommended,
                    enrich_is_mac, is_mac, mac_requirements_minimum, mac_requirements_recommended,
                    enrich_is_linux, is_linux, linux_requirements_minimum, linux_requirements_recommended
                    ) FROM STDIN WITH CSV""",
                buffer
            )
            
            # Perform UPSERT from temp table to main table
            cur.execute("""
                INSERT INTO staging.details (
                    appid, name, game_appid, 
                    enrich_is_description, long_description, short_description, 
                    image_header, image_background, image_screenshots, image_movies, 
                    enrich_is_price, is_free, currency, price,
                    type, categories, genres, supported_languages, controller_support, 
                    enrich_is_date, release_date, 
                    required_age, 
                    developers, publishers, content_descriptors_notes, 
                    achievements, 
                    enrich_is_windows, is_windows, windows_requirements_minimum, windows_requirements_recommended,
                    enrich_is_mac, is_mac, mac_requirements_minimum, mac_requirements_recommended,
                    enrich_is_linux, is_linux, linux_requirements_minimum, linux_requirements_recommended
                )
                SELECT 
                    *
                FROM temp_app_details
                ON CONFLICT (appid) 
                DO UPDATE SET 
                    name = EXCLUDED.name,
                    game_appid = EXCLUDED.game_appid,
                    enrich_is_description = EXCLUDED.enrich_is_description,
                    long_description = EXCLUDED.long_description,
                    short_description = EXCLUDED.short_description,
                    image_header = EXCLUDED.image_header,
                    image_background = EXCLUDED.image_background,
                    image_screenshots = EXCLUDED.image_screenshots,
                    image_movies = EXCLUDED.image_movies,
                    enrich_is_price = EXCLUDED.enrich_is_price,
                    is_free = EXCLUDED.is_free,
                    currency = EXCLUDED.currency,
                    price = EXCLUDED.price,
                    type = EXCLUDED.type,
                    categories = EXCLUDED.categories,
                    genres = EXCLUDED.genres,
                    supported_languages = EXCLUDED.supported_languages,
                    controller_support = EXCLUDED.controller_support,
                    enrich_is_date = EXCLUDED.enrich_is_date,
                    release_date = EXCLUDED.release_date,
                    required_age = EXCLUDED.required_age,
                    developers = EXCLUDED.developers,
                    publishers = EXCLUDED.publishers,
                    content_descriptors_notes = EXCLUDED.content_descriptors_notes,
                    achievements = EXCLUDED.achievements,
                    enrich_is_windows = EXCLUDED.enrich_is_windows,
                    is_windows = EXCLUDED.is_windows,
                    windows_requirements_minimum = EXCLUDED.windows_requirements_minimum,
                    windows_requirements_recommended = EXCLUDED.windows_requirements_recommended,
                    enrich_is_mac = EXCLUDED.enrich_is_mac,
                    is_mac = EXCLUDED.is_mac,
                    mac_requirements_minimum = EXCLUDED.mac_requirements_minimum,
                    mac_requirements_recommended = EXCLUDED.mac_requirements_recommended,
                    enrich_is_linux = EXCLUDED.enrich_is_linux,
                    is_linux = EXCLUDED.is_linux,
                    linux_requirements_minimum = EXCLUDED.linux_requirements_minimum,
                    linux_requirements_recommended = EXCLUDED.linux_requirements_recommended,
                    updated_at = NOW()
                ;
            """)
            
            conn.commit()
            print("App details updated successfully")
            
        except Exception as e:
            conn.rollback()
            raise Exception(f"Error updating app details: {str(e)}")
        
        finally:
            cur.close()
            conn.close()
            
    def upsert_app_tags(self, df_app_tags):
        pass
    
    def upsert_app_reviews(self, df_app_reviews):
        pass
    
    def enrich_app_details(self, df_app_details):
        pass