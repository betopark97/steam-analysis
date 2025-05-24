with exploded as (
    select
        distinct
        appid
        ,unnest(image_movies) as movie
    from {{ ref('details') }}
)
,numbered as (
    select
        appid
        ,row_number() over (order by movie) as movie_id
        ,movie as path
    from exploded
)

select * from numbered