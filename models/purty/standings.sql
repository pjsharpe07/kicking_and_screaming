SELECT games_agg.team_id
, teams.team_name
, teams.conference
, games_agg.num_home_games + games_agg.num_away_games AS games_played
, games_agg.points_home
, games_agg.points_away
, games_agg.points_home + games_agg.points_away AS total_points
, total_points / games_played AS points_per_game
, ROW_NUMBER() OVER(ORDER BY TOTAL_POINTS DESC) AS shield_standings
, ROW_NUMBER() OVER(PARTITION BY conference ORDER BY TOTAL_POINTS DESC) AS conference_standings
, ROW_NUMBER() OVER(ORDER BY points_per_game DESC) AS shield_ppg_standings
, ROW_NUMBER() OVER(PARTITION BY conference ORDER BY points_per_game DESC) AS conference_ppg_standings
FROM {{ ref('home_away_agg') }} games_agg
LEFT JOIN {{ ref('teams_clean') }} teams
ON games_agg.team_id = teams.team_id