with base as (
    select
        appid
        ,unnest(developers) as developer
    from {{ ref('v_app_details') }}
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
        developer_id
        ,lower(regexp_replace(trim(developer_name), '\s+', '', 'g')) as developer_normalized
        ,developer_name
    from {{ ref('developers') }}
)
,joined as (
    select
        distinct
        b.appid
        ,d.developer_id
    from normalized_base b
    join developers d
        on b.developer_normalized = d.developer_normalized
)

select * from joined