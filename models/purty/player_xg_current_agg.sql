SELECT *
FROM {{ ref('player_xg_agg') }}
QUALIFY 1 = ROW_NUMBER() OVER(PARTITION BY player_id ORDER BY matchday DESC)
