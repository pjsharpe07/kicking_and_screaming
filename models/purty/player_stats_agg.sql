with gplus_agg AS (
	SELECT player_id
	, player_name
	, team_id
	, team_name
	, season_name
	, total_gplus
	, gplus_league_rank
	FROM {{ ref('player_gplus_agg') }}
	QUALIFY 1 = ROW_NUMBER() OVER(PARTITION BY player_id, season_name)
) 
SELECT xg.player_id
, xg.player_name
, gp.team_id
, gp.team_name
, xg.season_name
, gp.total_gplus
, gp.gplus_league_rank
, xg.total_goals
, xg.total_xg
, xg.rolling_three_game_xgsum
, xg.rolling_five_game_xgsum
, xg.rolling_three_game_xgavg
, xg.rolling_five_game_xgavg
FROM {{ ref('player_xg_current_agg') }} xg
JOIN gplus_agg gp
ON xg.player_id = gp.player_id
AND xg.season_name = gp.season_name