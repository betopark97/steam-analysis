with base as (
    select
        '<name> ' || name || '</name>' || E'\n' ||
        '<short_description> ' || short_description || '</short_description>' || E'\n' ||
        '<long_description> ' || long_description || '</long_description>' || E'\n' ||
        '<content_descriptors_notes> ' || content_descriptors_notes || '</content_descriptors_notes>' || E'\n' ||
        '<categories> ' || array_to_string(categories, ', ') || '</categories>' || E'\n' ||
        '<genres> ' || array_to_string(categories, ', ') || '</genres>' || E'\n' ||
        '<supported_languages> ' || supported_languages || '</supported_languages>' || E'\n' ||
        '<required_age> ' || required_age || '</required_age>' AS document
        ,json_build_object(
            'appid', appid,
            'name', name,
            'short_description', short_description,
            'long_description', long_description,
            'content_descriptors_notes', content_descriptors_notes,
            'categories', categories,
            'genres', genres,
            'supported_languages', supported_languages,
            'required_age', required_age
        ) AS metadata
    from {{ ref('details') }}
    where type = 'game'
)
,metadata as (
    select
        *
        ,encode(digest(document, 'sha256'), 'hex') AS document_hash
        ,now() as updated_at 
    from base
)

select * from metadata