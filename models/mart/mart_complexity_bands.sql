with games as (
    select * from {{ ref('stg_bgg__games') }}
),

banded as (
    select
        case
            when complexity_weight < 2.0 then 'Light'
            when complexity_weight < 3.0 then 'Medium-Light'
            when complexity_weight < 4.0 then 'Medium-Heavy'
            else 'Heavy'
        end                             as complexity_band,
        count(*)                        as num_games,
        avg(avg_rating)                 as avg_rating,
        avg(num_user_ratings)           as avg_num_ratings,
        avg(num_owned)                  as avg_num_owned,
        min(complexity_weight)          as min_complexity,
        max(complexity_weight)          as max_complexity
    from games
    where complexity_weight is not null
    group by complexity_band
)

select * from banded
order by min_complexity asc