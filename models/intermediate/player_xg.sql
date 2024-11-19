SELECT xg.player_id
, p.player_name
, xg.season_name
, xg.game_id
, g.matchday
, xg.general_position
, xg.minutes_played
, xg.shots
, xg.shots_on_target
, xg.goals
, xg.xgoals
, xg.goals_minus_xgoals
FROM {{ source('raw', 'player_xg') }} xg
LEFT JOIN {{ source('raw', 'players') }} p
ON xg.player_id = p.player_id 
LEFT JOIN {{ source('raw', 'games') }} g
ON xg.game_id = g.game_id
