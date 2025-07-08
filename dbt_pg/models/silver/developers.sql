with exploded as (
    select
        unnest(developers) as developer
    from {{ ref('details') }}
)
,normalized as (
    select
        developer
        ,lower(regexp_replace(trim(developer), '\s+', '', 'g')) as developer_normalized
    from exploded
)
,deduplicated as (
    select
        developer_normalized
        ,min(developer) as developer_name
    from normalized
    group by developer_normalized
)
,numbered as (
    select
        row_number() over (order by developer_normalized) as developer_id
        ,developer_name
    from deduplicated
)

select developer_id, developer_name from numbered