"""
This will be a location to grab ddl's for raw table creation
"""

########## team data ###########

teams_table = """
CREATE TABLE IF NOT EXISTS raw.teams (
	team_id VARCHAR PRIMARY KEY,
	team_name VARCHAR,
	team_short_name	VARCHAR,
	team_abbreviation VARCHAR(10),
	league VARCHAR
)
"""

team_conference_table = """
CREATE TABLE IF NOT EXISTS raw.team_conferences (
    team_name VARCHAR,
    conference VARCHAR
)
"""

team_goals_added_table = """
CREATE TABLE IF NOT EXISTS raw.team_goals_added (
	team_id VARCHAR,
    minutes INTEGER,
    season_name INTEGER,
    data STRUCT(
		action_type VARCHAR, 
		num_actions_for INTEGER, 
		goals_added_for  REAL,
		num_actions_against INTEGER, 
		goals_added_against REAL)
)
"""

team_salaries_table = """
CREATE TABLE IF NOT EXISTS raw.team_salaries (
	team_id VARCHAR,
	season_name INTEGER,
	count_players INTEGER,
	total_guaranteed_compensation REAL,
	avg_guaranteed_compensation REAL,
	median_guaranteed_compensation REAL,
	std_dev_guaranteed_compensation REAL
)
"""

team_xg_table = """
CREATE TABLE IF NOT EXISTS raw.team_xg (
	team_id VARCHAR,
    season_name INTEGER,
	count_games INTEGER,
	shots_for INTEGER,
	shots_against INTEGER,
	goals_for INTEGER,
	goals_against INTEGER,
	goal_difference INTEGER,
	xgoals_for REAL,
	xgoals_against REAL,
	xgoal_difference REAL,
	goal_difference_minus_xgoal_difference REAL,
	points INTEGER,
	xpoints REAL,
)
"""

team_xpass_table = """
CREATE TABLE IF NOT EXISTS raw.team_xpass (
	team_id VARCHAR,
    season_name INTEGER,
	count_games INTEGER,
	attempted_passes_for INTEGER,
	pass_completion_percentage_for REAL,
	xpass_completion_percentage_for REAL,
	passes_completed_over_expected_for REAL,
	passes_completed_over_expected_p100_for REAL,
	avg_vertical_distance_for REAL,
	attempted_passes_against INTEGER,
	pass_completion_percentage_against REAL,
	xpass_completion_percentage_against REAL,
	passes_completed_over_expected_against REAL,
	passes_completed_over_expected_p100_against REAL,
	avg_vertical_distance_against REAL,
	passes_completed_over_expected_difference REAL,
	avg_vertical_distance_differenc REAL
)
"""

# not really sure where to put manager? Doing it here I guess

manager_table = """
CREATE TABLE IF NOT EXISTS raw.managers (
	manager_id VARCHAR,
	manager_name VARCHAR,
	nationality VARCHAR,
	competition VARCHAR
)
"""

########## game/stadium data data ###########


game_table = """
CREATE TABLE IF NOT EXISTS raw.games (
	game_id VARCHAR PRIMARY KEY,
	date_time_utc TIMESTAMPTZ,
	home_score INTEGER,
	away_score INTEGER,
	home_team_id VARCHAR,
	away_team_id VARCHAR,
	referee_id VARCHAR,
	stadium_id VARCHAR,
	home_manager_id VARCHAR,
	away_manager_id VARCHAR,
	expanded_minutes INTEGER,
	season_name INTEGER,
	matchday INTEGER,
	attendance HUGEINT,
	knockout_game BOOLEAN,
	last_updated_utc TIMESTAMPTZ,
)
"""

game_xg_table = """
CREATE TABLE IF NOT EXISTS raw.game_xg (
	game_id VARCHAR PRIMARY KEY,
	date_time_utc TIMESTAMPTZ,
	home_team_id VARCHAR,
	home_goals INTEGER,
	home_team_xgoals REAL,
	home_player_xgoals REAL,
	away_team_id VARCHAR,
	away_goals INTEGER,
	away_team_xgoals REAL,
	away_player_xgoals REAL,
	goal_difference INTEGER,
	team_xgoal_difference REAL,
	player_xgoal_difference REAL,
	final_score_difference INTEGER,
	home_xpoints REAL,
	away_xpoints REAL
)
"""

stadium_table = """
CREATE TABLE IF NOT EXISTS raw.stadiums (
	stadium_id VARCHAR PRIMARY KEY,
	stadium_name VARCHAR,
	year_built INTEGER,
	street VARCHAR,
	city VARCHAR,
	province VARCHAR,
	country VARCHAR,
	postal_code VARCHAR,
	capacity INTEGER,
	roof INTEGER,
	turf INTEGER,
	latitude REAL,
	longitude REAL,
	field_x INTEGER,
	field_y INTEGER,
	competition VARCHAR,
)
"""
