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