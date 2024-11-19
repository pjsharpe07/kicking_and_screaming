SELECT stand.team_id
, stand.season_name
, stand.team_name
, stand.conference 
, stand.total_points
, stand.points_per_game
, xg.xgoals_for
, xg.xgoals_against
, xg.xgoal_difference
, gplus.total_gplus
, gplus.avg_gplus_per_game
FROM {{ ref('standings') }} stand
LEFT JOIN {{ ref('team_xg_agg') }} xg
ON stand.team_id = xg.team_id
LEFT JOIN {{ ref('team_gplus_agg') }} gplus
ON stand.team_id = gplus.team_id