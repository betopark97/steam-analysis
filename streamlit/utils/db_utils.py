import os 
from dotenv import load_dotenv
load_dotenv()

def get_db_params():
    return {
        'host': os.environ.get('DB_HOST'),
        'user': os.environ.get('DB_USER'),
        'password': os.environ.get('DB_PASSWORD'),
        'database': os.environ.get('DB_NAME'),
        'port': os.environ.get('DB_PORT')
    }