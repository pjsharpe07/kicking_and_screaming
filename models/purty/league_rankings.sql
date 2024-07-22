SELECT stand.team_id
, stand.team_name
, stand.total_points
, stand.points_per_game
, stand.shield_standings
, stand.shield_ppg_standings
, xg.xgoals_for_rank
, xg.xgoals_against_rank
, xg.xgoal_diff_rank
, gplus.team_gplus_rank
, gplus.avg_gplus_rank
FROM {{ ref('standings') }} stand
LEFT JOIN {{ ref('team_xg_agg') }} xg
ON stand.team_id = xg.team_id
LEFT JOIN {{ ref('team_gplus_agg') }} gplus
ON stand.team_id = gplus.team_id
