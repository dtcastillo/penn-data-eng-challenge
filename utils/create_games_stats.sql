DROP TABLE IF EXISTS game_stats;
CREATE TABLE game_stats (
player_person_id int,
player_person_currentTeam_name varchar(50),
player_person_fullName varchar(50),
player_stats_skaterStats_assists float8,
player_stats_skaterStats_goals float8,
side varchar(50)
)
