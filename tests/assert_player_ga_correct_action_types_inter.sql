SELECT 
  player_id
, game_id
, COUNT(DISTINCT action_type)
FROM {{ ref('player_gplus') }}
GROUP BY 1,2 
HAVING COUNT(DISTINCT action_type) != 6
