WITH DATA AS (
	SELECT team.team_id
	, team.team_name
	, gp.action_type
	, gp.goals_added_for
	, gp.goals_added_against
	, gp.ga_diff
    FROM {{ ref('team_gplus') }} gp
	LEFT JOIN {{ ref('teams_clean') }} team
	ON gp.team_id = team.team_id
)
PIVOT data
ON action_type
USING SUM(goals_added_for) AS gplus_for
, SUM(goals_added_against) AS gplus_against
, SUM(ga_diff) AS gplus_diff
ORDER BY team_id
