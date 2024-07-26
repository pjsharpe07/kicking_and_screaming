with gplus_agg AS (
	SELECT team_id
	, SUM(goals_added_for) AS total_gplus
	, SUM(ga_diff) AS total_gplus_diff -- TODO: is this interesting?
	FROM {{ ref('team_gplus') }}
	WHERE action_type IN ('Interrupting', 'Claiming', 'Fouling')
	GROUP BY 1
) SELECT 
  teams.team_id
, teams.team_name
, teams.conference
, stand.games_played 
, tg.total_gplus
, tg.total_gplus / stand.games_played as avg_gplus_per_game
, ROW_NUMBER() OVER(ORDER BY total_gplus DESC) AS team_gplus_rank
, ROW_NUMBER() OVER(PARTITION BY teams.conference ORDER BY total_gplus DESC) AS team_gplus_conf_rank
, ROW_NUMBER() OVER(ORDER BY avg_gplus_per_game DESC) AS avg_gplus_rank
, ROW_NUMBER() OVER(PARTITION BY teams.conference ORDER BY avg_gplus_per_game DESC) AS avg_gplus_conf_rank
FROM gplus_agg tg
LEFT JOIN {{ ref('teams_clean') }} teams
ON tg.team_id = teams.team_id
LEFT JOIN {{ ref('standings') }} stand
ON tg.team_id = stand.team_id
ORDER BY avg_gplus_per_game DESC
