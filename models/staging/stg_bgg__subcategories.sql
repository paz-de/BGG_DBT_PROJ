with source as (
    select * from {{ source('bgg_raw', 'BGG_SUBCATEGORIES') }}
),

cleaned as (
    select
        BGG_ID          as game_id,
        SUBCATEGORY     as subcategory_name,
        _LOADED_AT      as loaded_at
    from source
    where BGG_ID is not null
      and SUBCATEGORY is not null
)

select * from cleaned