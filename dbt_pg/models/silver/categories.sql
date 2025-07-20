with exploded as (
    select
        distinct
        unnest(categories) as category
    from {{ ref('v_app_details') }}
)
,numbered as (
    select
        row_number() over (order by category) as category_id
        ,category as category_name
    from exploded
)

select * from numbered