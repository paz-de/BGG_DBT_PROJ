with games as (
    select * from {{ ref('stg_bgg__games') }}
),

final as (
    select
        game_id,
        game_name,
        year_published,
        complexity_weight,
        avg_rating,
        bayes_avg_rating,
        num_user_ratings,
        min_players,
        max_players,
        com_min_playtime,
        com_max_playtime,
        num_owned,
        num_expansions,
        is_kickstarted,
        game_family,
        rank_overall,
        rank_strategy,
        rank_family,
        rank_thematic,
        rank_war,
        rank_party,
        -- Complexity band for easy filtering in Tableau
        case
            when complexity_weight < 2.0 then 'Light'
            when complexity_weight < 3.0 then 'Medium-Light'
            when complexity_weight < 4.0 then 'Medium-Heavy'
            else 'Heavy'
        end as complexity_band
    from games
    where rank_overall is not null
    order by rank_overall asc
)

select * from final