"""
This DAG is responsible for backing up the project's databases.
"""
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.latest_only import LatestOnlyOperator

import os
from datetime import datetime, timedelta


# The backup path inside the Airflow container, as defined in docker-compose.yml
BACKUP_ROOT_PATH = "/opt/airflow/backups"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 6),
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
    'depend_on_past': True
}

with DAG(
    dag_id='db_backups',
    default_args=default_args,
    description='Make backups of the PostgreSQL and MongoDB databases',
    schedule="0 0 * * *",  # Daily at midnight
    catchup=False,
    max_active_runs=1,
) as dag:

    latest_only = LatestOnlyOperator(task_id='latest_only')

    # Task to ensure the backup subdirectories exist before running the backups.
    create_backup_dirs = BashOperator(
        task_id="create_backup_directories",
        bash_command=f"""
        echo "Creating backup directories if not exists..."
        mkdir -p {BACKUP_ROOT_PATH}/postgres && \
        mkdir -p {BACKUP_ROOT_PATH}/mongo
        """,
    )

    # Task to back up the MongoDB database.
    # It executes mongodump inside the 'steam_mongo' container and saves the gzipped output.
    mongo_backup = BashOperator(
        task_id="mongo_backup",
        bash_command=f"""
        echo "Starting MongoDB backup..."
        docker exec steam_mongo \
        mongodump \
            --username {os.getenv("MONGODB_INITDB_ROOT_USERNAME")} \
            --password '{os.getenv("MONGODB_INITDB_ROOT_PASSWORD")}' \
            --authenticationDatabase admin \
            --archive \
            --gzip \
            > {BACKUP_ROOT_PATH}/mongo/mongo_backup_$(date +%F_%H%M).gz
        echo "MongoDB backup complete."
        """,
    )
    
    # Task to back up the PostgreSQL database.
    # It executes pg_dumpall inside the 'steam_postgres' container and saves the gzipped output.
    postgres_backup = BashOperator(
        task_id="postgres_backup",
        bash_command=f"""
        echo "Starting PostgreSQL backup..."
        docker exec -t steam_postgres \
        pg_dumpall \
            -U {os.getenv("PGDB_USER")} | \
            gzip > {BACKUP_ROOT_PATH}/postgres/postgres_backup_$(date +%F_%H%M).sql.gz
        echo "PostgreSQL backup complete."
        """,
    )

    # Task to clean up old backups, keeping only the 3 most recent ones.
    cleanup_backups = BashOperator(
        task_id="cleanup_backups",
        bash_command=f"""
        echo "Cleaning up old backups..."
        
        # Clean up PostgreSQL backups
        ls -t {BACKUP_ROOT_PATH}/postgres | tail -n +4 | while read -r f; do rm -- "{BACKUP_ROOT_PATH}/postgres/$f"; done
        
        # Clean up MongoDB backups
        ls -t {BACKUP_ROOT_PATH}/mongo | tail -n +4 | while read -r f; do rm -- "{BACKUP_ROOT_PATH}/mongo/$f"; done
        
        echo "Cleanup complete."
        """,
    )

    # Define the task execution order.
    latest_only >> create_backup_dirs
    create_backup_dirs >> [postgres_backup, mongo_backup] >> cleanup_backups
