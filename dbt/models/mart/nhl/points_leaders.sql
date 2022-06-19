with players as (
    select 
        *,
        rank() over (partition by team_name order by points desc) as rk
    from {{ ref('nhl_players') }})

select 
    team_name,
    full_name,
    points
from players
where rk = 1 and points > 0