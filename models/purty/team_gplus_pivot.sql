WITH DATA AS (
	SELECT team.team_id
	, team.team_name
	, team.conference
	, gp.season_name
	, gp.action_type
	, gp.goals_added_for
	, gp.goals_added_against
	, gp.ga_diff
    FROM {{ ref('team_gplus') }} gp
	LEFT JOIN {{ ref('teams_clean') }} team
	ON gp.team_id = team.team_id
), pivoted AS (
	PIVOT data
	ON action_type
	USING SUM(goals_added_for) AS gplus_for
	, SUM(goals_added_against) AS gplus_against
	, SUM(ga_diff) AS gplus_diff
	ORDER BY team_id
) 
SELECT team_id
, team_name
, conference
, season_name
, Claiming_gplus_for AS claiming_gplus
, ROW_NUMBER() OVER(PARTITION BY season_name ORDER BY Claiming_gplus_for DESC) AS claiming_gplus_for_rank
, Claiming_gplus_against AS claming_gplus_against
, ROW_NUMBER() OVER(PARTITION BY season_name ORDER BY Claiming_gplus_against DESC) AS claiming_gplus_against_rank
, Claiming_gplus_diff AS claiming_gplus_diff
, ROW_NUMBER() OVER(PARTITION BY season_name ORDER BY Claiming_gplus_diff DESC) AS claiming_gplus_diff_rank
, Dribbling_gplus_for AS dribbling_gplus_for
, ROW_NUMBER() OVER(PARTITION BY season_name  ORDER BY Dribbling_gplus_for DESC) AS dribbling_gplus_for_rank
, Dribbling_gplus_against AS dribbling_gplus_against
, ROW_NUMBER() OVER(PARTITION BY season_name  ORDER BY Dribbling_gplus_against DESC) AS dribbling_gplus_against_rank
, Dribbling_gplus_diff AS dribbling_gplus_diff
, ROW_NUMBER() OVER(PARTITION BY season_name ORDER BY Dribbling_gplus_diff DESC) AS dribbling_gplus_diff_rank
, Fouling_gplus_for AS fouling_gplus_for
, ROW_NUMBER() OVER(PARTITION BY season_name ORDER BY Fouling_gplus_for DESC) AS fouling_gplus_for_rank
, Fouling_gplus_against AS fouling_gplus_against
, ROW_NUMBER() OVER(PARTITION BY season_name ORDER BY Fouling_gplus_against DESC) AS fouling_gplus_against_rank
, fouling_gplus_diff AS fouling_gplus_diff
, ROW_NUMBER() OVER(PARTITION BY season_name ORDER BY fouling_gplus_diff DESC) AS fouling_gplus_diff_rank
, Interrupting_gplus_for AS interrupting_gplus_for
, ROW_NUMBER() OVER(PARTITION BY season_name ORDER BY Interrupting_gplus_for DESC) AS interrupting_gplus_for_rank
, interrupting_gplus_against
, ROW_NUMBER() OVER(PARTITION BY season_name ORDER BY Interrupting_gplus_against DESC) AS interrupting_gplus_against_rank
, Interrupting_gplus_diff AS interrupting_gplus_diff
, ROW_NUMBER() OVER(PARTITION BY season_name ORDER BY Interrupting_gplus_diff DESC) AS interrupting_gplus_diff_rank
, Passing_gplus_for AS passing_gplus_for
, ROW_NUMBER() OVER(PARTITION BY season_name ORDER BY Passing_gplus_for DESC) AS passing_gplus_for_rank
, Passing_gplus_against AS passing_gplus_against
, ROW_NUMBER() OVER(PARTITION BY season_name ORDER BY Passing_gplus_against DESC) AS passing_gplus_against_rank
, Passing_gplus_diff AS passing_gplus_diff
, ROW_NUMBER() OVER(PARTITION BY season_name ORDER BY Passing_gplus_diff DESC) AS passing_gplus_diff_rank
, Receiving_gplus_for AS receiving_gplus_for
, ROW_NUMBER() OVER(PARTITION BY season_name ORDER BY Receiving_gplus_for DESC) AS receiving_gplus_for_rank
, Receiving_gplus_against AS receiving_gplus_against
, ROW_NUMBER() OVER(PARTITION BY season_name ORDER BY Receiving_gplus_against DESC) AS receiving_gplus_against_rank
, Receiving_gplus_diff AS receiving_gplus_diff
, ROW_NUMBER() OVER(PARTITION BY season_name ORDER BY Receiving_gplus_diff DESC) AS receiving_gplus_diff_rank
, Shooting_gplus_for AS shooting_gplus_for
, ROW_NUMBER() OVER(PARTITION BY season_name ORDER BY Shooting_gplus_for DESC) AS shooting_gplus_for_rank
, Shooting_gplus_against AS shooting_gplus_against
, ROW_NUMBER() OVER(PARTITION BY season_name ORDER BY Shooting_gplus_against DESC) AS shooting_gplus_against_rank
, Shooting_gplus_diff AS shooting_gplus_diff
, ROW_NUMBER() OVER(PARTITION BY season_name ORDER BY Shooting_gplus_diff DESC) AS shooting_gplus_diff_rank
FROM pivoted