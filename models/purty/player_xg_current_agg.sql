SELECT player_id
, player_name
, matchday
, season_name
, general_position
, SUM(goals) OVER(PARTITION BY player_id, season_name) AS total_goals
, SUM(xgoals) OVER(PARTITION BY player_id, season_name) AS total_xg
, rolling_three_game_xgsum
, rolling_five_game_xgsum
, rolling_three_game_xgavg
, rolling_five_game_xgavg
FROM {{ ref('player_xg_agg') }}
QUALIFY 1 = ROW_NUMBER() OVER(PARTITION BY player_id, season_name ORDER BY matchday DESC)
