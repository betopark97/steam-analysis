from managers.steam_api_manager import SteamAPIManager


def extract_app_names(steam_api_manager: SteamAPIManager, app_names):
    return steam_api_manager.get_app_names(app_names)

def extract_app_details(steam_api_manager: SteamAPIManager, app_id):
    return steam_api_manager.get_app_details(app_id)

def extract_app_tags(steam_api_manager: SteamAPIManager, app_id):
    return steam_api_manager.get_app_tags(app_id)

def extract_app_reviews(steam_api_manager: SteamAPIManager, app_id):
    return steam_api_manager.get_app_reviews(app_id)