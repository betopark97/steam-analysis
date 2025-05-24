with exploded as (
    select
        distinct
        unnest(genres) as genre
    from {{ ref('details') }}
)
,numbered as (
    select
        row_number() over (order by genre) as id
        ,genre as name
    from exploded
)

select * from numbered