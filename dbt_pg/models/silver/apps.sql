with base as (
    select
        appid, name, game_appid, 
        enrich_is_description, long_description, short_description,
        image_header, image_background, image_screenshots, image_movies, 
        enrich_is_price, is_free, currency, price, 
        type, supported_languages, controller_support,
        enrich_is_date, release_date, 
        required_age, 
        publishers, content_descriptors_notes,
        achievements, 
        enrich_is_windows, is_windows, windows_requirements_minimum, windows_requirements_recommended,
        enrich_is_mac, is_mac, mac_requirements_minimum, mac_requirements_recommended,
        enrich_is_linux, is_linux, linux_requirements_minimum, linux_requirements_recommended
    from {{ ref('details') }}
)

select * from base