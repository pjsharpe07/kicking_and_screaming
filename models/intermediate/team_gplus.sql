SELECT team_id
, minutes
, season_name
, data.action_type as action_type
, data.num_actions_for as goals_added_for
, data.goals_added_for as goals_added_above_avg
, data.num_actions_against as num_actions_against
, data.goals_added_against AS goals_added_against
, (data.goals_added_for - data.goals_added_against) AS ga_diff
FROM {{ source('raw', 'team_goals_added') }}
