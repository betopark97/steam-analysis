# Managers
from managers.mongo_manager import MongoManager
from managers.postgres_manager import PostgresManager
from utils.utils_pipeline import (
    convert_mixed_columns_to_string,
    to_pg_array_str_safe,
    remove_html_tags_df,
    normalize_strings_df,
    remove_plus_sign_df,
    filter_age_df,
    is_diff_texts_df,
    clean_descriptions_df,
)
# Ignore warnings
from bs4 import MarkupResemblesLocatorWarning
import warnings


warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

