WITH home_data AS (
	SELECT home_team_id AS team_id
	, SUM(home_points) AS team_home_points
	, COUNT(*) AS home_game_count
	FROM {{ ref('games_with_points') }}
	GROUP BY 1
), away_data AS (
	SELECT away_team_id AS team_id
	, SUM(away_points) AS team_away_points
	, COUNT(*) AS away_game_count
	FROM {{ ref('games_with_points') }}
	GROUP BY 1
) SELECT home.team_id
, teams.team_name
, conf.conference
, home.home_game_count + away.AWAY_GAME_COUNT AS games_played
, home.team_home_points
, away.team_away_points
, home.team_home_points + away.team_away_points AS total_points
, total_points / games_played AS points_per_game
FROM home_data home
LEFT JOIN away_data away
ON home.team_id = away.team_id
LEFT JOIN {{ source('raw', 'teams') }} teams
ON home.team_id = teams.team_id
LEFT JOIN {{ source('raw', 'team_conferences') }} conf
ON teams.team_name = conf.team_name
