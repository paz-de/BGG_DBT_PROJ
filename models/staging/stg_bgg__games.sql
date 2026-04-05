with source as (
    select * from {{ source('bgg_raw', 'BGG_GAMES') }}
),

renamed as (
    select
        BGG_ID                          as game_id,
        NAME                            as game_name,
        YEAR_PUBLISHED                  as year_published,
        GAME_WEIGHT                     as complexity_weight,
        AVG_RATING                      as avg_rating,
        BAYES_AVG_RATING                as bayes_avg_rating,
        NUM_USER_RATINGS                as num_user_ratings,
        MIN_PLAYERS                     as min_players,
        MAX_PLAYERS                     as max_players,
        MFG_PLAY_TIME                   as mfg_play_time,
        COM_MIN_PLAYTIME                as com_min_playtime,
        COM_MAX_PLAYTIME                as com_max_playtime,
        NUM_OWNED                       as num_owned,
        NUM_WANT                        as num_want,
        NUM_WISH                        as num_wish,
        NUM_EXPANSIONS                  as num_expansions,
        KICKSTARTED                     as is_kickstarted,
        FAMILY                          as game_family,
        RANK_BOARDGAME                  as rank_overall,
        RANK_STRATEGY                   as rank_strategy,
        RANK_FAMILY                     as rank_family,
        RANK_THEMATIC                   as rank_thematic,
        RANK_WAR                        as rank_war,
        RANK_PARTY                      as rank_party,
        RANK_ABSTRACT                   as rank_abstract,
        RANK_CGS                        as rank_cgs,
        RANK_CHILDRENS                  as rank_childrens,
        _LOADED_AT                      as loaded_at
    from source
    where BGG_ID is not null
      and NAME is not null
)

select * from renamed