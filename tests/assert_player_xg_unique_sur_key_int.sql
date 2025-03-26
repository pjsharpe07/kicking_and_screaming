SELECT 
  player_id
, game_id
, COUNT(*)
FROM {{ ref('player_xg') }} --test intermediate model
GROUP BY 1,2 
HAVING COUNT(*) > 1