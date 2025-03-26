SELECT 
  player_id
, game_id
, COUNT(*)
FROM {{ source('raw', 'player_xg') }}
GROUP BY 1,2 
HAVING COUNT(*) > 1