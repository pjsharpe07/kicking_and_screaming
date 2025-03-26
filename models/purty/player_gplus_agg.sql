with player_gplus_agg AS (
	SELECT player_id
	, player_name
	, team_id
	, team_name
	, season_name
	, general_position
	, minutes_played
	, action_type
	, SUM(goals_added_raw) OVER(PARTITION BY player_id, season_name) AS total_gplus
	, SUM(goals_added_raw) OVER(PARTITION BY player_id, season_name, action_type) AS gplus_action_type
	FROM {{ ref('player_gplus') }}
	QUALIFY 1 = ROW_NUMBER() OVER(PARTITION BY player_id, season_name, action_type)
) SELECT *
, DENSE_RANK() OVER(PARTITION BY season_name ORDER BY total_gplus DESC)  AS gplus_league_rank
, DENSE_RANK() OVER(PARTITION BY season_name, action_type ORDER BY gplus_action_type DESC)  AS gplus_league_action_rank
, DENSE_RANK() OVER(PARTITION BY team_id, season_name ORDER BY total_gplus DESC)  AS gplus_team_rank
, DENSE_RANK() OVER(PARTITION BY team_id, season_name, action_type ORDER BY gplus_action_type DESC)  AS gplus_team_action_rank
FROM player_gplus_agg
ORDER BY gplus_league_rank
