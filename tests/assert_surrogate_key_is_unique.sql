SELECT team_id
, season_name
, data.action_type
, COUNT(*) COUNT
FROM {{ source('raw', 'team_goals_added') }} 
GROUP BY 1,2,3
HAVING COUNT > 1
