with source as (
    select * from {{ source('bgg_raw', 'BGG_MECHANICS') }}
),

cleaned as (
    select
        BGG_ID      as game_id,
        MECHANIC    as mechanic_name,
        _LOADED_AT  as loaded_at
    from source
    where BGG_ID is not null
      and MECHANIC is not null
)

select * from cleaned