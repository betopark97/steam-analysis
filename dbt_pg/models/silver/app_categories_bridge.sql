with base as (
    select
        appid
        ,unnest(categories) as category
    from bronze.details
)
,joined as (
    select
        distinct
        b.appid
        ,c.id as category_id
    from base b
    join {{ ref('categories') }} c
        on b.category = c.name
)

select * from joined