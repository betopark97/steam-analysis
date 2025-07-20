select
    *
from {{ source('staging', 'app_tags')}}