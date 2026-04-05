with games as (
    select * from {{ ref('stg_bgg__games') }}
),

mechanics as (
    select * from {{ ref('stg_bgg__mechanics') }}
),

joined as (
    select
        g.game_id,
        g.game_name,
        g.avg_rating,
        g.complexity_weight,
        g.rank_overall,
        g.num_user_ratings,
        m.mechanic_name
    from games g
    inner join mechanics m on g.game_id = m.game_id
)

select * from joined