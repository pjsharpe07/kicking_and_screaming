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
SELECT 
  COALESCE(home.team_id, away.team_id) AS team_id
, COALESCE(home.season_name, away.season_name) AS season_name
, COALESCE(home.num_home_games, 0) AS num_home_games
, COALESCE(home.points_home, 0) AS points_home
, COALESCE(away.num_away_games , 0) AS num_away_games
, COALESCE(away.points_away, 0) AS points_away
FROM home_games home
FULL OUTER JOIN away_games away
ON home.team_id = away.team_id 
AND home.season_name = away.season_name