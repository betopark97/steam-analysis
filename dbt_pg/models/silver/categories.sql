with exploded as (
    select
        unnest(categories) as category
    from bronze.details
)
, distinct_categories as (
    select
        distinct category
    from exploded
)

select
    row_number() over (order by category) as id
    ,category as name
from distinct_categories