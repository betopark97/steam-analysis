from typing import Any
import difflib
from bs4 import BeautifulSoup
import unicodedata
from collections import Counter
import numpy as np
import pandas as pd
import polars as pl
from datetime import datetime

#--------------------------------------
# Clean Schemas
#--------------------------------------
def get_deep_type(val: Any) -> str:
    """Recursively detect the type inside lists or dicts."""
    if isinstance(val, list):
        if not val:
            return 'list[empty]'
        return f"list[{', '.join(sorted({get_deep_type(v) for v in val}))}]"
    elif isinstance(val, dict):
        return "dict"
    elif pd.isna(val):
        return "null"
    else:
        return type(val).__name__

def infer_column_types(df: pd.DataFrame, sample_size: int = 1000) -> dict:
    """Check deep types of values in a DataFrame."""
    type_report = {}

    for col in df.columns:
        types = Counter()
        for val in df[col].dropna().head(sample_size):
            try:
                dtype = get_deep_type(val)
                types[dtype] += 1
            except Exception as e:
                types[f"Error: {str(e)}"] += 1

        type_report[col] = dict(types)

    return type_report

def convert_mixed_columns_to_string(df: pd.DataFrame, sample_size: int = 1000) -> pd.DataFrame:
    """Convert columns with mixed types to string."""
    type_info = infer_column_types(df, sample_size)
    mixed_columns = [col for col, types in type_info.items() if len(types) > 1]

    for col in mixed_columns:
        df[col] = df[col].apply(lambda x: str(x) if not (x is None or isinstance(x, float) and np.isnan(x)) else None)

    return df

def to_pg_array_str_safe(value):
    """
    Converts a list of strings to a properly escaped PostgreSQL array literal.
    Handles quotes, commas, and special characters.
    """
    def escape_pg_string(s):
        # Escape backslashes first
        s = s.replace('\\', '\\\\')
        # Escape double quotes by doubling them
        s = s.replace('"', '\\"')  # use \\" instead of double double-quotes
        return f'"{s}"'

    return "{" + ",".join(escape_pg_string(v) for v in value) + "}"


#--------------------------------------
# Clean Strings
#--------------------------------------
def remove_html_tags(text: str):
    if isinstance(text, str):
        soup = BeautifulSoup(text or "", "html.parser")
        return soup.get_text(separator=" ").strip()
    return text

def remove_html_tags_df(df: pl.LazyFrame, cols: list) -> pl.LazyFrame:
    return df.with_columns([
        pl.col(col).map_elements(remove_html_tags, return_dtype=pl.String).alias(col)
        for col in cols
    ])
    
def normalize_strings(text: str) -> str:
    if isinstance(text, str):
        return unicodedata.normalize('NFKC', text).strip()
    return text

def normalize_strings_df(df: pl.LazyFrame, cols: list) -> pl.LazyFrame:
    return df.with_columns([
        pl.col(col).map_elements(normalize_strings, return_dtype=pl.String).alias(col)
        for col in cols
    ])
    
def strip_list_strings_df(df: pl.LazyFrame, cols: list) -> pl.LazyFrame:
    return df.with_columns([
        pl.col(col).list.eval(pl.element().str.strip_chars().alias(col))
        for col in cols
    ])
    
#--------------------------------------
# Clean Ages
#--------------------------------------
def remove_plus_sign_df(df: pl.LazyFrame, cols: list) -> pl.LazyFrame:
    return df.with_columns([
        pl.col(col).cast(pl.String).str.replace_all(r'\+', '').alias(col)
        for col in cols
    ])
    
def convert_age_to_int_df(df: pl.LazyFrame, cols: list) -> pl.LazyFrame:
    return df.with_columns([
        pl.col(col)
        .cast(pl.Int8, strict=False)  # coerce errors to null
        .fill_null(0)
        .alias(col)
        for col in cols
    ])
    
def filter_age(age: int) -> int:
    if isinstance(age, int):
        return 0 if age < 0 or age > 21 else age
    return age
    
def filter_age_df(df: pl.LazyFrame, cols: list) -> pl.LazyFrame:
    return df.with_columns([
        pl.col(col).map_elements(filter_age, return_dtype=pl.Int8).alias(col)
        for col in cols
    ])
    
#--------------------------------------
# Clean Descriptions
#--------------------------------------

def is_diff_texts(a: str, b: str) -> bool:
    a_lines = (a or "").splitlines()
    b_lines = (b or "").splitlines()
    diff = difflib.ndiff(a_lines, b_lines)
    return bool(any(line.startswith(('- ', '+ ')) for line in diff))

#--------------------------------------
# Clean Descriptions
#--------------------------------------

def parse_steam_review_html(html: str) -> list:
    
    soup = BeautifulSoup(html, 'html.parser')
    review_boxes = soup.find_all('div', class_='review_box')

    review_dicts = []
    for box in review_boxes:
        # -- Author Information --
        author_name = box.find('div', class_='persona_name').get_text(strip=True) if box.find('div', class_='persona_name') else None
        author_owned_games = box.find('div', class_='num_owned_games').get_text(strip=True) if box.find('div', class_='num_owned_games') else None
        author_reviews_posted = box.find('div', class_='num_reviews').get_text(strip=True) if box.find('div', class_='num_reviews') else None
        author_hours_on_record = box.find('div', class_='hours').get_text(strip=True) if box.find('div', class_='hours') else None
        # -- Post Information --
        posted_date = box.find('div', class_='postedDate').get_text(strip=True).strip().replace('Posted:', '').replace('Direct from Steam', '') + f", {datetime.now().year}" if box.find('div', class_='postedDate') else None
        # -- Review Content --
        review_text = box.find('div', class_='content').get_text(separator='\n', strip=True) if box.find('div', class_='content') else None
        # -- Vote Information --
        votes_helpful = box.find('div', class_='vote_info').get_text(strip=True) if box.find('div', class_='vote_info') else None
        review_dict = {
            "author_name": author_name,
            "owned_games": author_owned_games,
            "reviews_posted": author_reviews_posted,
            "hours_on_record": author_hours_on_record,
            "posted_date": posted_date,
            "review_text": review_text,
            "votes_helpful": votes_helpful
        }
        review_dicts.append(review_dict)
    return review_dicts

def parse_steam_review_html_df(df: pl.LazyFrame, col: str) -> pl.LazyFrame:
    review_struct = pl.Struct([
        pl.Field("author_name", pl.String),
        pl.Field("owned_games", pl.String),
        pl.Field("reviews_posted", pl.String),
        pl.Field("hours_on_record", pl.String),
        pl.Field("posted_date", pl.String),
        pl.Field("review_text", pl.String),
        pl.Field("votes_helpful", pl.String),
    ])

    return df.with_columns([
        pl.col(col)
        .map_elements(parse_steam_review_html, return_dtype=pl.List(review_struct))
        .alias(col)
    ])

def parse_steam_review_score(review_score: str) -> dict:
    soup = BeautifulSoup(review_score, 'html.parser')
    span = soup.find('span', class_='game_review_summary')
    
    if not span:
        return {
            "review_sentiment": None,
            "review_score_text": None
        }
    
    review_score_sentiment = span.get_text(strip=True) if span else None
    review_score_text = span.get('data-tooltip-html') if span else None

    review_score_dict = {
        "review_sentiment": review_score_sentiment,
        "review_score_text": review_score_text,
    }
    return review_score_dict

def parse_steam_review_score_df(df: pl.LazyFrame, col: str) -> pl.LazyFrame:
    review_struct = pl.Struct([
        pl.Field("review_sentiment", pl.String),
        pl.Field("review_score_text", pl.String),
    ])
    
    return df.with_columns([
        pl.col(col)
        .map_elements(parse_steam_review_score, return_dtype=review_struct)
        .alias(col)
    ])