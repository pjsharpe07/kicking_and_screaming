with home_games AS (
	SELECT home_team_id AS team_id
	, season_name
	, COUNT(*) AS num_home_games
	, SUM(home_points) AS points_home
	FROM {{ ref('games_with_points') }}
	GROUP BY 1, 2
), away_games AS (
	SELECT away_team_id AS team_id
	, season_name
	, COUNT(*) AS num_away_games
	, SUM(away_points) AS points_away
	FROM {{ ref('games_with_points') }}
	GROUP BY 1, 2
)
	SELECT home.*
	, away.* EXCLUDE (team_id, season_name)
	FROM home_games home
	LEFT JOIN away_games away
	ON home.team_id = away.team_id 
	AND home.season_name = away.season_name