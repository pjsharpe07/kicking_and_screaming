WITH home_away_analysis AS (
    SELECT *
    , points_home / num_home_games AS avg_points_home
    , points_away / num_away_games AS avg_points_away
    FROM {{ ref('home_away_agg') }} 
) 
SELECT 
haa.team_id
, teams.team_name
, haa.* EXCLUDE (team_id)
, ROW_NUMBER() OVER(ORDER BY avg_points_home DESC) AS rank_home
, ROW_NUMBER() OVER(ORDER BY avg_points_away DESC) AS rank_away
FROM home_away_analysis haa
LEFT JOIN {{ source('raw', 'teams') }} teams
ON haa.team_id = teams.team_id