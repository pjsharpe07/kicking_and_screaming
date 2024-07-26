SELECT team.*
, conf.conference
FROM {{ source('raw', 'teams') }} team
LEFT JOIN {{ source('raw', 'team_conferences') }} conf
ON team.team_name = conf.team_name