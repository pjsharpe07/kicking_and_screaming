SELECT 
xg.team_id
, teams.team_name
, teams.conference
, xg.season_name
, xg.shots_for
, xg.shots_against
, xg.goals_for
, xg.goals_against
, xg.goal_difference
, xg.xgoals_for
, xg.xgoals_against
, xg.xgoal_difference
, ROW_NUMBER() OVER(PARTITION BY season_name ORDER BY xgoals_for DESC) AS xgoals_for_rank
, ROW_NUMBER() OVER(PARTITION BY conference, season_name ORDER BY xgoals_for DESC) AS xgoals_for_conf_rank
, ROW_NUMBER() OVER(PARTITION BY season_name ORDER BY xgoals_against DESC) AS xgoals_against_rank
, ROW_NUMBER() OVER(PARTITION BY conference, season_name ORDER BY xgoals_against DESC) AS xgoals_against_conf_rank
, ROW_NUMBER() OVER(PARTITION BY season_name ORDER BY xgoal_difference DESC) AS xgoal_diff_rank
, ROW_NUMBER() OVER(PARTITION BY conference, season_name ORDER BY xgoal_difference DESC) AS xgoal_diff_conf_rank
FROM {{ source('raw', 'team_xg')  }} xg
LEFT JOIN {{ ref('teams_clean') }} teams
ON xg.team_id = teams.team_id
ORDER BY xgoal_difference DESC
