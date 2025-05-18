select
    *
from {{ source('mongodb_polars', 'details')}}