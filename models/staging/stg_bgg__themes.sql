with source as (
    select * from {{ source('bgg_raw', 'BGG_THEMES') }}
),

cleaned as (
    select
        BGG_ID      as game_id,
        THEME       as theme_name,
        _LOADED_AT  as loaded_at
    from source
    where BGG_ID is not null
      and THEME is not null
)

select * from cleaned