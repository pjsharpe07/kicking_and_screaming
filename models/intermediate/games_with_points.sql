SELECT game_id
, season_name
, home_team_id
, away_team_id
, CASE
	WHEN home_score > away_score THEN 3
	WHEN home_score = away_score THEN 1
	ELSE 0
END AS home_points
, CASE
	WHEN away_score > home_score THEN 3
	WHEN home_score = away_score THEN 1
	ELSE 0
END AS away_points
FROM {{ source('raw', 'games') }}

