with base as (
    select
        appid
        ,unnest(categories) as category
    from {{ ref('details') }}
)
,joined as (
    select
        distinct
        b.appid
        ,c.category_id
    from base b
    join {{ ref('categories') }} c
        on b.category = c.category_name
)

select * from joined