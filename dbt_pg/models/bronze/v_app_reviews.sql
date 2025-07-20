select
    *
from {{ source('staging', 'app_reviews')}}