with exploded as (
    select
        distinct
        appid
        ,unnest(image_screenshots) as screenshot
    from {{ ref('details') }}
)
,numbered as (
    select
        appid
        ,row_number() over (order by screenshot) as screenshot_id
        ,screenshot as path
    from exploded
)

select * from numbered