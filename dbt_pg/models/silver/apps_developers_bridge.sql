with base as (
    select
        appid
        ,unnest(developers) as developer
    from {{ ref('details') }}
)
,normalized_base as (
    select
        appid
        ,developer
        ,lower(regexp_replace(trim(developer), '\s+', '', 'g')) as developer_normalized
    from base
)
,developers as (
    select
        id
        ,lower(regexp_replace(trim(name), '\s+', '', 'g')) as developer_normalized
        ,name
    from {{ ref('developers') }}
)
,joined as (
    select
        distinct
        b.appid
        ,d.id as developer_id
    from normalized_base b
    join developers d
        on b.developer_normalized = d.developer_normalized
)

select * from joined