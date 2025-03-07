SELECT stand.team_id
, stand.team_name
, stand.season_name
, stand.total_points
, stand.points_per_game
, stand.shield_standings
, stand.shield_ppg_standings
, xg.xgoals_for_rank
, xg.xgoals_against_rank
, xg.xgoal_diff_rank
, gplus.team_gplus_rank
, gplus.avg_gplus_rank
, gplus_piv.claiming_gplus_for_rank
, gplus_piv.claiming_gplus_against_rank
, gplus_piv.claiming_gplus_diff_rank
, gplus_piv.dribbling_gplus_for_rank
, gplus_piv.dribbling_gplus_against_rank
, gplus_piv.dribbling_gplus_diff_rank
, gplus_piv.fouling_gplus_for_rank
, gplus_piv.fouling_gplus_against_rank
, gplus_piv.fouling_gplus_diff_rank
, gplus_piv.interrupting_gplus_for_rank
, gplus_piv.interrupting_gplus_against_rank
, gplus_piv.interrupting_gplus_diff_rank
, gplus_piv.passing_gplus_for_rank
, gplus_piv.passing_gplus_against_rank
, gplus_piv.passing_gplus_diff_rank
, gplus_piv.receiving_gplus_for_rank
, gplus_piv.receiving_gplus_against_rank
, gplus_piv.receiving_gplus_diff_rank
, gplus_piv.shooting_gplus_for_rank
, gplus_piv.shooting_gplus_against_rank
, gplus_piv.shooting_gplus_diff_rank
FROM {{ ref('standings') }} stand
LEFT JOIN {{ ref('team_xg_agg') }} xg
    ON stand.team_id = xg.team_id
    AND stand.season_name = xg.season_name
LEFT JOIN {{ ref('team_gplus_agg') }} gplus
    ON stand.team_id = gplus.team_id
    AND stand.season_name = gplus.season_name
LEFT JOIN {{ ref('team_gplus_pivot') }} gplus_piv
    ON stand.team_id = gplus_piv.team_id
    AND stand.season_name = gplus_piv.season_name