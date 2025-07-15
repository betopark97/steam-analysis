from data_pipeline.pipeline_stg_details import (
    get_stg_details_filtered_ids,
    process_stg_details_filtered
)
from data_pipeline.pipeline_stg_tags import (
    get_stg_tags_filtered_ids,
    process_stg_tags_filtered
)
from data_pipeline.pipeline_stg_reviews import (
    get_stg_reviews_filtered_appids,
    process_stg_reviews_filtered
)
from data_pipeline.managers.mongo_manager import MongoManager
from data_pipeline.managers.postgres_manager import PostgresManager
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.latest_only import LatestOnlyOperator
from datetime import datetime, timedelta


import warnings
import logging
from bs4 import MarkupResemblesLocatorWarning

# TODO: This code to prevent beautifulsoup warnings is not working. Need to investigate.
# Step 1: Prevent warning from being raised
warnings.simplefilter("ignore", category=MarkupResemblesLocatorWarning)

# Step 2: Prevent it from being logged by Airflow
class MarkupResemblesLocatorFilter(logging.Filter):
    def filter(self, record):
        return "MarkupResemblesLocatorWarning" not in record.getMessage()

# Attach the filter to root logger
for handler in logging.getLogger().handlers:
    handler.addFilter(MarkupResemblesLocatorFilter())

# TODO: have to make it so that the updated_at in postgres is KST
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 8),
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
    'depend_on_past': True
}

mongo_manager = MongoManager()
postgres_manager = PostgresManager()

with DAG(
    dag_id='mongo_to_postgres',
    default_args=default_args,
    description='Fetch Steam data and store it in MongoDB',
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    latest_only = LatestOnlyOperator(task_id='latest_only')

    # TODO: Change this to get appids instead of ids
    get_detail_ids = PythonOperator(
        task_id='get_stg_details_filtered_ids',
        python_callable=get_stg_details_filtered_ids,
        op_kwargs={
            'mongo_manager': mongo_manager
        }
    )

    process_details = PythonOperator(
        task_id='process_stg_details_filtered',
        python_callable=process_stg_details_filtered,
        op_kwargs={
            'mongo_manager': mongo_manager,
            'postgres_manager': postgres_manager,
            'filtered_str_ids': get_detail_ids.output
        }
    )

    # TODO: Change this to get appids instead of ids
    get_tags_ids = PythonOperator(
        task_id='get_stg_tags_filtered_ids',
        python_callable=get_stg_tags_filtered_ids,
        op_kwargs={
            'mongo_manager': mongo_manager
        }
    )

    process_tags = PythonOperator(
        task_id='process_stg_tags_filtered',
        python_callable=process_stg_tags_filtered,
        op_kwargs={
            'mongo_manager': mongo_manager,
            'postgres_manager': postgres_manager,
            'filtered_str_ids': get_tags_ids.output
        }
    )
    
    get_reviews_appids = PythonOperator(
        task_id='get_stg_reviews_filtered_appids',
        python_callable=get_stg_reviews_filtered_appids,
        op_kwargs={
            'mongo_manager': mongo_manager
        }
    )
    
    process_reviews = PythonOperator(
        task_id='process_stg_reviews_filtered',
        python_callable=process_stg_reviews_filtered,
        op_kwargs={
            'mongo_manager': mongo_manager,
            'postgres_manager': postgres_manager,
            'filtered_appids': get_reviews_appids.output
        }
    )

    # Define the task dependencies
    latest_only >> [get_detail_ids, get_tags_ids, get_reviews_appids]
    get_detail_ids >> process_details
    get_tags_ids >> process_tags
    get_reviews_appids >> process_reviews