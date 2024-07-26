SELECT team_id
, data.action_type
, COUNT(*) COUNT
FROM {{ source('raw', 'team_goals_added') }} 
GROUP BY 1,2
HAVING COUNT > 1
