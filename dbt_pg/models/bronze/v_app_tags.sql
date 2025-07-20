select
    *
from {{ source('mongodb_polars', 'app_tags')}}