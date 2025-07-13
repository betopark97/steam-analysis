import os
import psycopg2
import polars as pl


class PostgresManager:
    """Handle PostgreSQL database operations"""
    
    def __init__(self):
        # Connect to the database
        self.db_params = {
            'host': os.getenv('PGDB_HOST'),
            'user': os.getenv('PGDB_USER'),
            'password': os.getenv('PGDB_PASSWORD'),
            'database': os.getenv('PGDB_NAME'),
            'port': os.getenv('PGDB_PORT')
        }

    def upsert_app_details(self, df_app_details: pl.DataFrame):
        conn = psycopg2.connect(**self.db_params)
        cur = conn.cursor()
        
        try:
            # Prepare your UPSERT query template with placeholders
            upsert_query = """
                INSERT INTO staging.app_details (
                    appid, name, game_appid, 
                    enrich_is_description, long_description, short_description, 
                    image_header, image_background, image_screenshots, image_movies, 
                    enrich_is_price, is_free, currency, price,
                    type, categories, genres, supported_languages, is_controller_support, 
                    enrich_is_date, release_date, 
                    required_age, 
                    developers, publishers, content_descriptors_notes, 
                    achievements, 
                    enrich_is_windows, is_windows, windows_requirements_minimum, windows_requirements_recommended,
                    enrich_is_mac, is_mac, mac_requirements_minimum, mac_requirements_recommended,
                    enrich_is_linux, is_linux, linux_requirements_minimum, linux_requirements_recommended,
                    updated_at
                )
                VALUES (
                    %(appid)s, %(name)s, %(game_appid)s,
                    %(enrich_is_description)s, %(long_description)s, %(short_description)s,
                    %(image_header)s, %(image_background)s, %(image_screenshots)s, %(image_movies)s,
                    %(enrich_is_price)s, %(is_free)s, %(currency)s, %(price)s,
                    %(type)s, %(categories)s, %(genres)s, %(supported_languages)s, %(is_controller_support)s,
                    %(enrich_is_date)s, %(release_date)s,
                    %(required_age)s,
                    %(developers)s, %(publishers)s, %(content_descriptors_notes)s,
                    %(achievements)s,
                    %(enrich_is_windows)s, %(is_windows)s, %(windows_requirements_minimum)s, %(windows_requirements_recommended)s,
                    %(enrich_is_mac)s, %(is_mac)s, %(mac_requirements_minimum)s, %(mac_requirements_recommended)s,
                    %(enrich_is_linux)s, %(is_linux)s, %(linux_requirements_minimum)s, %(linux_requirements_recommended)s,
                    (NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Seoul')
                )
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
                    is_controller_support = EXCLUDED.is_controller_support,
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
                    updated_at = (NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Seoul')
            """
            
            # Iterate over each row of Polars DataFrame
            for row in df_app_details.iter_rows(named=True):
                # Note: Convert Polars-specific types to Python native if needed here
                cur.execute(upsert_query, row)
            
            conn.commit()
        
        except Exception as e:
            conn.rollback()
            raise Exception(f"Error updating app details row-by-row: {str(e)}")
        
        finally:
            cur.close()
            conn.close()
            
    def upsert_app_tags(self, df_app_tags: pl.DataFrame):
        conn = psycopg2.connect(**self.db_params)
        cur = conn.cursor()
        
        try:
            # Prepare your UPSERT query template with placeholders
            upsert_query = """
                INSERT INTO staging.app_tags (
                    appid, tags, updated_at
                )
                VALUES (
                    %(appid)s, %(tags)s, (NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Seoul')
                )
                ON CONFLICT (appid)
                DO UPDATE SET
                    tags = EXCLUDED.tags,
                    updated_at = (NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Seoul')
            """
            
            # Iterate over each row of Polars DataFrame
            for row in df_app_tags.iter_rows(named=True):
                # Note: Convert Polars-specific types to Python native if needed here
                cur.execute(upsert_query, row)
            
            conn.commit()
        
        except Exception as e:
            conn.rollback()
            raise Exception(f"Error updating app tags row-by-row: {str(e)}")
        
        finally:
            cur.close()
            conn.close()
    
    def upsert_app_reviews(self, df_app_reviews: pl.DataFrame):
        conn = psycopg2.connect(**self.db_params)
        cur = conn.cursor()
        
        try:
            # Prepare your UPSERT query template with placeholders
            upsert_query = """
                INSERT INTO staging.app_reviews (
                    appid, html, review_score, updated_at
                )
                VALUES (
                    %(appid)s, %(html)s, %(review_score)s, (NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Seoul')
                )
                ON CONFLICT (appid)
                DO UPDATE SET
                    html = EXCLUDED.html,
                    review_score = EXCLUDED.review_score,
                    updated_at = (NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Seoul')
            """
            
            # Iterate over each row of Polars DataFrame
            for row in df_app_reviews.iter_rows(named=True):
                # Note: Convert Polars-specific types to Python native if needed here
                cur.execute(upsert_query, row)
            
            conn.commit()
        
        except Exception as e:
            conn.rollback()
            raise Exception(f"Error updating app reviews row-by-row: {str(e)}")
        
        finally:
            cur.close()
            conn.close()
    
    def enrich_app_details(self, df_app_details):
        pass