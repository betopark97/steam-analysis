with exploded as (
    select
        unnest(genres) as genre
    from bronze.details
)
, distinct_genres as (
    select
        distinct genre
    from exploded
)

select
    row_number() over (order by genre) as id
    ,genre as name
from distinct_genres