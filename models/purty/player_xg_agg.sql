SELECT player_id
, player_name
, matchday
, general_position
, goals
, xgoals
, SUM(xgoals) OVER(
	PARTITION BY player_id
	ORDER BY matchday
	ROWS BETWEEN 3 PRECEDING AND 0 FOLLOWING 
) AS rolling_three_game_xgsum
, SUM(xgoals) OVER(
	PARTITION BY xg.player_id
	ORDER BY matchday
	ROWS BETWEEN 5 PRECEDING AND 0 FOLLOWING 
) AS rolling_five_game_xgsum
, AVG(xgoals) OVER(
	PARTITION BY xg.player_id
	ORDER BY matchday
	ROWS BETWEEN 5 PRECEDING AND 0 FOLLOWING 
) AS rolling_three_game_xgavg
, AVG(xgoals) OVER(
	PARTITION BY player_id
	ORDER BY matchday
	ROWS BETWEEN 5 PRECEDING AND 0 FOLLOWING 
) AS rolling_five_game_xgavg
FROM {{ ref('player_xg') }} xg
