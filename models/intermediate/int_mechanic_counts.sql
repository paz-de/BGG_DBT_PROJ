with mechanics as (
    select * from {{ ref('stg_bgg__mechanics') }}
),

games as (
    select * from {{ ref('stg_bgg__games') }}
),

joined as (
    select
        m.mechanic_name,
        count(distinct m.game_id)       as num_games,
        avg(g.avg_rating)               as avg_rating,
        avg(g.complexity_weight)        as avg_complexity,
        avg(g.num_user_ratings)         as avg_num_ratings
    from mechanics m
    inner join games g on m.game_id = g.game_id
    group by m.mechanic_name
)

select * from joined
order by num_games desc