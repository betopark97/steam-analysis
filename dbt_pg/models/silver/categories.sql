with exploded as (
    select
        distinct
        unnest(categories) as category
    from {{ ref('details') }}
)
,numbered as (
    select
        row_number() over (order by category) as id
        ,category as name
    from exploded
)

select * from numbered