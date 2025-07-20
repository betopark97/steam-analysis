select
    *
from {{ source('staging', 'app_details')}}