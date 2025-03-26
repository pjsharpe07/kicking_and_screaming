SELECT 
  player_id
, game_id
, COUNT(DISTINCT data.action_type)
FROM {{ source('raw', 'player_goals_added') }}
GROUP BY 1,2 
HAVING COUNT(DISTINCT data.action_type) != 6