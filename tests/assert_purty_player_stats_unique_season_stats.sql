SELECT 
  player_id
, season_name
, COUNT(*) AS total_records
FROM {{ ref('player_stats_agg') }}
GROUP BY 1, 2 
HAVING COUNT(*) > 1
