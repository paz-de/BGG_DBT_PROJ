with themes as (
    select * from {{ ref('stg_bgg__themes') }}
),

games as (
    select * from {{ ref('stg_bgg__games') }}
),

joined as (
    select
        t.theme_name,
        count(distinct t.game_id)       as num_games,
        avg(g.avg_rating)               as avg_rating,
        avg(g.complexity_weight)        as avg_complexity
    from themes t
    inner join games g on t.game_id = g.game_id
    group by t.theme_name
)

select * from joined
order by num_games desc