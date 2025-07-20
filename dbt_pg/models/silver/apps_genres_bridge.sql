with base as (
    select
        appid
        ,unnest(genres) as genre
    from {{ ref('v_app_details') }}
)
,joined as (
    select
        distinct
        b.appid
        ,g.genre_id
    from base b
    join {{ ref('genres') }} g
        on b.genre = g.genre_name
)

select * from joined