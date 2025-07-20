with exploded as (
    select
        distinct
        unnest(genres) as genre
    from {{ ref('v_app_details') }}
)
,numbered as (
    select
        row_number() over (order by genre) as genre_id
        ,genre as genre_name
    from exploded
)

select * from numbered