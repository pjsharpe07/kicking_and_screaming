SELECT pga.player_id
, p.player_name
, pga.team_id
, t.team_name
, g.game_id
, g.matchday
, pga.* EXCLUDE(player_id, team_id, game_id, data)
, pga.data.action_type
, pga.data.goals_added_raw
, pga.data.goals_added_above_avg
, pga.data.count_actions
FROM {{ source('raw', 'player_goals_added') }} pga
LEFT JOIN {{ source('raw', 'players') }} p
ON pga.player_id = p.player_id
LEFT JOIN {{ source('raw', 'teams') }} t
ON pga.team_id = t.team_id
LEFT JOIN {{ source('raw', 'games') }} g
ON pga.game_id = g.game_id