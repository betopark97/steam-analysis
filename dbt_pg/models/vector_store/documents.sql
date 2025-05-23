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
        ,jsonb_build_object(
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
,hashed_documents as (
    select
        document,
        metadata || jsonb_build_object('document_hash', encode(digest(document, 'sha256'), 'hex')) AS metadata
    from base
)

select * from hashed_documents