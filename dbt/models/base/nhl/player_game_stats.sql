select
        player_person_id as nhl_player_id,
        side,
        player_person_fullName full_name,
        player_person_currentTeam_name game_team_name,
        player_stats_skaterStats_assists stats_assists,
        player_stats_skaterStats_goals stats_goals
  from {{ source('nhl', 'game_stats') }}


