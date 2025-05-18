with base as (
    select
        appid
        ,unnest(genres) as genre
    from {{ ref('details') }}
)
,joined as (
    select
        distinct
        b.appid
        ,g.id as genre_id
    from base b
    join {{ ref('genres') }} g
        on b.genre = g.name
)

select * from joined