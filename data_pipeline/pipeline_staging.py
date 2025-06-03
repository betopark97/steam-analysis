# System
from pathlib import Path
from dotenv import load_dotenv
import importlib

# Data Management
import json
import re
import difflib
from bs4 import BeautifulSoup
import unicodedata
from collections import Counter

# Data Science
import numpy as np
import pandas as pd
import polars as pl

# Managers
from managers.mongo_manager import AsyncMongoManager
from managers.postgres_manager import PostgresManager

# Ignore warnings
from bs4 import MarkupResemblesLocatorWarning
import warnings

warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

load_dotenv()

