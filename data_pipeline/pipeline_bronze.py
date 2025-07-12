import logging
from collections import defaultdict
from datetime import datetime
from data_pipeline.managers.steam_api_manager import SteamAPIManager
from data_pipeline.managers.mongo_manager import MongoManager
from data_pipeline.utils.utils_logging import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def fetch_and_store_app_names():
    """Fetch and store app names from Steam API."""
    steam_api_manager = SteamAPIManager()
    mongo_manager = MongoManager()
    app_names = steam_api_manager.get_app_names()
    mongo_manager.upsert_app_names(app_names)

def get_filtered_appids() -> list:
    mongo_manager = MongoManager()
    db = mongo_manager.database
    names_col = db['names']
    details_col = db['details']
    no_details_col = db['no_details']
    tags_col = db['tags']
    reviews_col = db['reviews']

    # Step 1: Get all appids from 'names' excluding 'playtest'
    base_cursor = names_col.find(
        {"name": {"$not": {"$regex": r"\bplaytest\b", "$options": "i"}}},
        {"appid": 1, "datetime": 1}
    )
    name_docs = [doc for doc in base_cursor]
    base_appids = [doc["appid"] for doc in name_docs]

    # Step 2: Existing appids from other collections
    existing_details = set([doc["appid"] for doc in details_col.find({}, {"appid": 1})])
    existing_no_details = set([doc["appid"] for doc in no_details_col.find({}, {"appid": 1})])
    existing_tags = set([doc["appid"] for doc in tags_col.find({}, {"appid": 1})])
    existing_reviews = set([doc["appid"] for doc in reviews_col.find({}, {"appid": 1})])

    details_union = existing_details | existing_no_details

    # Step 3: Score appids based on how many collections they are missing from
    priority_scores = defaultdict(list)
    appid_to_datetime = {}

    for doc in name_docs:
        appid = doc["appid"]
        appid_to_datetime[appid] = doc.get("datetime", datetime.now())
        missing_count = 0
        if appid not in details_union:
            missing_count += 1
        if appid not in existing_tags:
            missing_count += 1
        if appid not in existing_reviews:
            missing_count += 1

        priority_scores[missing_count].append(appid)

    # Step 4: Build final sorted list of appids by priority level
    final_appids = []

    for score in [3, 2, 1]:
        for appid in priority_scores[score]:
            if len(final_appids) < 1000:
                final_appids.append(appid)
            else:
                break

    already_selected = set(final_appids)

    # Step 5: Oldest remaining appids by datetime (score = 0)
    remaining_appids = [
        (appid, appid_to_datetime[appid])
        for appid in base_appids
        if appid not in already_selected
    ]

    oldest_sorted = sorted(remaining_appids, key=lambda x: x[1])

    for appid, _ in oldest_sorted:
        if len(final_appids) < 1000:
            final_appids.append(appid)
        else:
            break

    return final_appids

def fetch_and_store_app_details(steam_api_manager: SteamAPIManager, mongo_manager: MongoManager, appids: list):
    """Fetch and store app details for a given appid."""
    logger.info(f"Starting to fetch details for {len(appids)} appids...")
    if not isinstance(appids, list):
        logger.error(f"Expected a list of appids, got {type(appids)}: {appids}")
        raise ValueError("Expected a list of appids.")

    for appid in appids:
        details = steam_api_manager.get_app_details(appid)
        mongo_manager.upsert_app_details(appid, details)

def fetch_and_store_app_tags(steam_api_manager: SteamAPIManager, mongo_manager: MongoManager, appids: list):
    """Fetch and store app tags for a given appid."""
    logger.info(f"Starting to fetch tags for {len(appids)} appids...")
    if not isinstance(appids, list):
        logger.error(f"Expected a list of appids, got {type(appids)}: {appids}")
        raise ValueError("Expected a list of appids.")

    for appid in appids:
        tags = steam_api_manager.get_app_tags(appid)
        if tags is None:
            logger.warning(f"JSONDecodeError fetching tags for appid {appid}. Skipping this appid.")
            continue
        mongo_manager.upsert_app_tags(appid, tags)

def fetch_and_store_app_reviews(steam_api_manager: SteamAPIManager, mongo_manager: MongoManager, appids: list):
    """Fetch and store app reviews for a given appid."""
    logger.info(f"Starting to fetch reviews for {len(appids)} appids...")
    if not isinstance(appids, list):
        logger.error(f"Expected a list of appids, got {type(appids)}: {appids}")
        raise ValueError("Expected a list of appids.")

    for appid in appids:
        reviews = steam_api_manager.get_app_reviews(appid)
        mongo_manager.upsert_app_reviews(appid, reviews)