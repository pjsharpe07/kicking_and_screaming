"""
This will be a location to grab ddl's for raw table creation
"""

########## game/stadium data data ###########

player_table = """
CREATE TABLE IF NOT EXISTS raw.players (
	player_id VARCHAR,
	player_name VARCHAR,
	birth_date DATE,
	nationality VARCHAR,
	primary_broad_position VARCHAR,
	primary_general_position VARCHAR,
	secondary_broad_position VARCHAR,
	secondary_general_position VARCHAR,
	-- kind of a weird one. Some data is lists, some empty obects, some string of years
    season_name VARCHAR, 
	height_ft REAL,
	height_in REAL,
	weight_lb REAL,
	competition VARCHAR,
)
"""

player_ga_table = """
CREATE TABLE IF NOT EXISTS raw.player_goals_added (
	player_id VARCHAR,
    season_name INTEGER,
	game_id VARCHAR,
	team_id VARCHAR,
	general_position VARCHAR,
	minutes_played INTEGER,
	data STRUCT(
    	action_type VARCHAR,
        goals_added_raw REAL,
        goals_added_above_avg REAL,
        count_actions INTEGER
    )
)
"""

player_xg_table = """
CREATE TABLE IF NOT EXISTS raw.player_xg (
	player_id VARCHAR,
    season_name INTEGER,
	game_id VARCHAR,
	team_id VARCHAR,
	general_position VARCHAR,
	minutes_played INTEGER,
	shots INTEGER,
	shots_on_target INTEGER,
	goals INTEGER,
	xgoals REAL,
	xplace REAL,
	goals_minus_xgoals REAL,
	key_passes INTEGER,
	primary_assists INTEGER,
	xassists REAL,
	primary_assists_minus_xassists REAL,
	xgoals_plus_xassists REAL,
	points_added REAL,
	xpoints_added REAL,
)
"""

player_xpass_table = """
CREATE TABLE IF NOT EXISTS raw.player_xpass (
	player_id VARCHAR,
    season_name INTEGER,
	game_id VARCHAR,
	team_id VARCHAR,
	general_position VARCHAR,
	minutes_played INTEGER,
	attempted_passes INTEGER,
	pass_completion_percentage REAL,
	xpass_completion_percentage REAL,
	passes_completed_over_expected REAL,
	passes_completed_over_expected_p100 REAL,
	avg_distance_yds  REAL,
	avg_vertical_distance_yds REAL,
	share_team_touches REAL,
)
"""

player_salary_table = """
CREATE TABLE IF NOT EXISTS raw.player_salaries (
	player_id VARCHAR,
	team_id VARCHAR,
	season_name INTEGER,
	position VARCHAR,
	base_salary REAL,
	guaranteed_compensation REAL,
	mlspa_releas DATE
)
"""

goalie_xg_table = """
CREATE TABLE IF NOT EXISTS raw.goalie_xg (
	player_id VARCHAR,
    season_name INTEGER,
	game_id VARCHAR,
	team_id VARCHAR,
	minutes_played INTEGER,
	shots_faced INTEGER,
	goals_conceded INTEGER,
	saves INTEGER,
	share_headed_shots REAL,
	xgoals_gk_faced REAL,
	goals_minus_xgoals_gk REAL,
	goals_divided_by_xgoals_gk REAL
)
"""

goalie_ga_table = """
CREATE TABLE IF NOT EXISTS raw.goalie_goals_added (
	player_id VARCHAR,
    season_name INTEGER,
	game_id VARCHAR,
	team_id VARCHAR,
	minutes_played INTEGER,
	data STRUCT(
		action_type VARCHAR,
		goals_added_raw REAL,
		goals_added_above_avg REAL,
		count_actions INTEGER
	)
)
"""
