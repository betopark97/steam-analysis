from managers.mongo_manager import MongoManager
from managers.postgres_manager import PostgresManager


def load_app_names_to_mongo(mongo_manager: MongoManager):
    return mongo_manager.upsert_app_names()

def load_app_details_to_mongo(mongo_manager: MongoManager, app_id):
    return mongo_manager.upsert_app_details(app_id)

def load_app_tags_to_mongo(mongo_manager: MongoManager, app_id):
    return mongo_manager.upsert_app_tags(app_id)

def load_app_reviews_to_mongo(mongo_manager: MongoManager, app_id):
    return mongo_manager.upsert_app_reviews(app_id)

def load_app_details_to_postgres(postgres_manager: PostgresManager, app_id):
    return postgres_manager.upsert_app_details(app_id)

def load_app_tags_to_postgres(postgres_manager: PostgresManager, app_id):
    return postgres_manager.upsert_app_tags(app_id)

def load_app_reviews_to_postgres(postgres_manager: PostgresManager, app_id):
    return postgres_manager.upsert_app_reviews(app_id)