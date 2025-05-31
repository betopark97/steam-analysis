from pathlib import Path
import sys
import logging
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
from collections import defaultdict
from datetime import datetime
import asyncio
from managers.steam_api_manager import AsyncSteamAPIManager
from managers.mongo_manager import AsyncMongoManager


load_dotenv()

def setup_logging():
    # Resolve the directory of this script file
    log_dir = Path(__file__).resolve().parent
    log_path = log_dir / "app.log"
    
    # Clear existing handlers
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # Set up log file rotation: max 200MB, keep 2 backup files
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=200 * 1024 * 1024,  
        backupCount=2,              
        encoding='utf-8'
    )

    stream_handler = logging.StreamHandler(sys.stdout)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[stream_handler, file_handler],
    )

    # Module-specific log levels
    logging.getLogger("AsyncMongoManager").setLevel(logging.INFO)
    logging.getLogger("AsyncSteamAPIManager").setLevel(logging.INFO)

    # Silence noisy libraries
    logging.getLogger("pymongo").setLevel(logging.WARNING)
    logging.getLogger("pymongo.topology").setLevel(logging.WARNING)
    logging.getLogger("motor").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)

setup_logging()
logger = logging.getLogger(__name__)

async def fetch_and_store_app_names(api_manager, mongo_manager):
    app_names = await api_manager.get_app_names()
    await mongo_manager.upsert_app_names(app_names)

async def get_filtered_appids(mongo_manager):
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
    name_docs = [doc async for doc in base_cursor]
    base_appids = [doc["appid"] for doc in name_docs]

    # Step 2: Existing appids from other collections
    existing_details = set([doc["appid"] async for doc in details_col.find({}, {"appid": 1})])
    existing_no_details = set([doc["appid"] async for doc in no_details_col.find({}, {"appid": 1})])
    existing_tags = set([doc["appid"] async for doc in tags_col.find({}, {"appid": 1})])
    existing_reviews = set([doc["appid"] async for doc in reviews_col.find({}, {"appid": 1})])

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

    # Step 6: Just fill any remaining to make up 1000
    if len(final_appids) < 1000:
        extras = [
            appid for appid in base_appids
            if appid not in set(final_appids)
        ]
        for appid in extras:
            if len(final_appids) < 1000:
                final_appids.append(appid)
            else:
                break

    return final_appids

async def fetch_and_store_app_details(api_manager, mongo_manager, appid):
    details = await api_manager.get_app_details(appid)
    await mongo_manager.upsert_app_details(appid, details)

async def fetch_and_store_tags_reviews(api_manager, mongo_manager, appid):
    tags_task = api_manager.get_app_tags(appid)
    reviews_task = api_manager.get_app_reviews(appid)

    tags, reviews = await asyncio.gather(tags_task, reviews_task)

    await asyncio.gather(
        mongo_manager.upsert_app_tags(appid, tags),
        mongo_manager.upsert_app_reviews(appid, reviews)
    )

async def main():
    api_manager = AsyncSteamAPIManager()
    mongo_manager = AsyncMongoManager()

    try:
        # 1. Fetch all the appids, names
        logger.info("Fetching app names from Steam API...")
        await fetch_and_store_app_names(api_manager, mongo_manager)

        # 2. Filter for prioritized appids
        logger.info("Filtering appids and prioritizing missing data...")
        appids = await get_filtered_appids(mongo_manager)

        # 3. Fetch details, tags, and reviews
        logger.info("Starting detail, tag, and review processing...")
        for idx, appid in enumerate(appids, start=1):
            try:
                logger.info(f"[{idx}/{len(appids)}] Processing AppID: {appid}")

                # Step 1: Get and store app details
                await fetch_and_store_app_details(api_manager, mongo_manager, appid)

                # Step 2: Get and store tags/reviews concurrently
                await fetch_and_store_tags_reviews(api_manager, mongo_manager, appid)

            except Exception as e:
                logger.error(f"[ERROR] AppID {appid} failed with exception: {e}", exc_info=True)

        logger.info("All appids processed.")

    except Exception as e:
        logger.critical(f"Unhandled exception in main(): {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())