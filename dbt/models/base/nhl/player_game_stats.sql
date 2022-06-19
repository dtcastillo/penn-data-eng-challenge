with game_stats as (
    select
        *,
        row_number() over (partition by game_id, player_person_id order by insert_timestamp desc) as rn
    from {{ source('nhl', 'game_stats') }}
)

select
    {{ dbt_utils.surrogate_key(['game_id', 'player_person_id']) }} as player_game_stats_key,
    game_id,
    player_person_id as nhl_player_id,
    side,
    player_person_fullName as full_name,
    player_person_currentTeam_name as game_team_name,
    coalesce(player_stats_skaterStats_assists, 0) as stats_assists,
    coalesce(player_stats_skaterStats_goals, 0) as stats_goals
from game_stats
where rn = 1