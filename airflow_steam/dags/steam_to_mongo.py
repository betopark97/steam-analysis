from airflow import DAG
from airflow.operators.python import PythonOperator
from plugins.utils.utils_dags import skip_if_not_latest_run
from data_pipeline.pipeline_bronze import (
    fetch_and_store_app_names,
    get_filtered_appids,
    fetch_and_store_app_details,
    fetch_and_store_app_tags,
    fetch_and_store_app_reviews,
)
from data_pipeline.managers.steam_api_manager import SteamAPIManager
from data_pipeline.managers.mongo_manager import MongoManager
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 3),
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
    'depends_on_past': True,
}

steam_api_manager = SteamAPIManager()
mongo_manager = MongoManager()

with DAG(
    dag_id='steam_to_mongo',
    default_args=default_args,
    description='Fetch Steam data and store it in MongoDB',
    schedule="0 */2 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:

    run_latest_only = PythonOperator(
        task_id='run_latest_only',
        python_callable=skip_if_not_latest_run
    )

    fetch_app_names = PythonOperator(
        task_id='fetch_app_names',
        python_callable=fetch_and_store_app_names,
        op_kwargs={
            'steam_api_manager': steam_api_manager, 
            'mongo_manager': mongo_manager
        }
    )

    filtered_appids = PythonOperator(
        task_id='filtered_appids',
        python_callable=get_filtered_appids,
        op_kwargs={
            'steam_api_manager': steam_api_manager, 
            'mongo_manager': mongo_manager
        }
    )

    fetch_app_details = PythonOperator(
        task_id='fetch_app_details',
        python_callable=fetch_and_store_app_details,
        op_kwargs={
            'steam_api_manager': steam_api_manager, 
            'mongo_manager': mongo_manager, 
            'appids': filtered_appids.output
        }
    )

    fetch_app_tags = PythonOperator(
        task_id='fetch_app_tags',
        python_callable=fetch_and_store_app_tags,
        op_kwargs={
            'steam_api_manager': steam_api_manager, 
            'mongo_manager': mongo_manager, 
            'appids': filtered_appids.output
        }
    )

    fetch_app_reviews = PythonOperator(
        task_id='fetch_app_reviews',
        python_callable=fetch_and_store_app_reviews,
        op_kwargs={
            'steam_api_manager': steam_api_manager, 
            'mongo_manager': mongo_manager, 
            'appids': filtered_appids.output
        }
    )

    # Define the task dependencies
    (
        run_latest_only 
        >> fetch_app_names
        >> filtered_appids
        >> [fetch_app_details, fetch_app_tags, fetch_app_reviews]
    )