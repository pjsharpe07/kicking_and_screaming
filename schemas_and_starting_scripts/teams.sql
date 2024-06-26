CREATE TABLE IF NOT EXISTS main.teams (
	team_id VARCHAR PRIMARY KEY,
	team_name VARCHAR,
	team_short_name	VARCHAR,
	team_abbreviation VARCHAR(10),
	league VARCHAR,
	conference VARCHAR,
)
