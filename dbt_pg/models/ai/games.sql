with base as (
    select
        appid, name,
        long_description, 
        encode(digest(long_description, 'sha256'), 'hex') AS hash_long_description,
        short_description,
        encode(digest(short_description, 'sha256'), 'hex') AS hash_short_description,
        content_descriptors_notes
    from {{ ref('details') }}
    where type = 'game'
)

select * from base