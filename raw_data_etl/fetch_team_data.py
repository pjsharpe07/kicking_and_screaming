import duckdb
from itscalledsoccer.client import AmericanSoccerAnalysis
from pandas import DataFrame
import os
from schemas_and_starting_scripts.raw_teams_tables import (
    teams_table,
    team_conference_table,
    team_goals_added_table,
    team_salaries_table,
    team_xg_table,
    team_xpass_table,
    manager_table,
)
from utils import (
    check_for_existing_data,
    create_schema_if_not_exists,
    filter_values_from_df,
    delete_matching_pkeys_stmt,
)
from tqdm import tqdm

# check for season name value
if 'season_name' not in globals():
    season_name = int(input("Which season do you want to pull data? "))

print(f"[teams_etl] Pulling team data for season: {season_name}")


path_to_database = os.path.join(os.getcwd(), "data", "kicking_dev.db")

# make the data directory if it doesn't exist
os.makedirs(os.path.dirname(path_to_database), exist_ok=True)


cursor = duckdb.connect(path_to_database)

# create the schema if it doesn't exist
create_schema_if_not_exists(schema="raw", cursor=cursor)

# now create the raw tables
cursor.execute(teams_table)
print("[teams_etl] Teams table created/exists")
cursor.execute(team_conference_table)
print("[teams_etl] Conference table created/exists")
cursor.execute(team_goals_added_table)
print("[teams_etl] Team Goals added table created/exists")
cursor.execute(team_salaries_table)
print("[teams_etl] Team Salary table created/exists")
cursor.execute(team_xg_table)
print("[teams_etl] Team XG data table created/exists")
cursor.execute(team_xpass_table)
print("[teams_etl] Team Xpass table created/exists")
cursor.execute(manager_table)
print("[teams_etl] Manager table created/exists")


# we will load this raw data, and move this transformation into dbt eventually
team_conferences = {
    "San Jose Earthquakes": "West",
    "New England Revolution": "East",
    "Philadelphia Union": "East",
    "Real Salt Lake": "West",
    "New York Red Bulls": "East",
    "CF Montréal": "East",
    "D.C. United": "East",
    "Los Angeles FC": "West",
    "Austin FC": "West",
    "Seattle Sounders FC": "West",
    "Orlando City SC": "East",
    "LA Galaxy": "West",
    "Atlanta United FC": "East",
    "Toronto FC": "East",
    "Minnesota United FC": "West",
    "Vancouver Whitecaps FC": "West",
    "FC Dallas": "West",
    "Columbus Crew": "East",
    "Charlotte FC": "East",
    "FC Cincinnati": "East",
    "Colorado Rapids": "West",
    "New York City FC": "East",
    "Nashville SC": "East",
    "Portland Timbers FC": "West",
    "St. Louis City SC": "West",
    "Chicago Fire FC": "East",
    "Houston Dynamo FC": "West",
    "Sporting Kansas City": "West",
    "Inter Miami CF": "East",
}

# pretty gross way to do this, but some existing was already live
team_names = list(team_conferences.keys())
team_conferences = list(team_conferences.values())

conference_df = DataFrame()
conference_df["team_name"] = team_names
conference_df["conference"] = team_conferences


conf_data_present = check_for_existing_data(
    schema="raw", table_name="team_conferences", cursor=cursor
)

# filter out team conference data if it already exists
if conf_data_present:
    conference_df = filter_values_from_df(
        source_table="kicking_dev.raw.team_conferences",
        target_df=conference_df,
        checking_column="team_name",
        cursor=cursor,
    )

cursor.sql("INSERT INTO raw.team_conferences SELECT * FROM conference_df")
print(f"[teams_etl] Inserted {len(conference_df):,} into raw.team_conferences")


# now start fetching the raw data from ASA

asa_client = AmericanSoccerAnalysis()

team_data_present = check_for_existing_data(
    schema="raw", table_name="teams", cursor=cursor
)

team_data = asa_client.get_teams(leagues="mls")

# only add new teams here
if team_data_present:
    team_data = filter_values_from_df(
        source_table="kicking_dev.raw.teams",
        target_df=team_data,
        checking_column="team_id",
        cursor=cursor,
    )

cursor.sql("INSERT INTO raw.teams SELECT * FROM team_data")
print(f"[teams_etl] Inserted {len(team_data):,} into teams table")


# team goals added -- we have to unnest the data for this one

team_ga_data_present = check_for_existing_data(
    schema="raw", table_name="team_goals_added", cursor=cursor
)

team_ga_data = asa_client.get_team_goals_added(leagues="mls", season_name=season_name)

if team_ga_data_present and len(team_ga_data) > 0:
    # duckdb doesn't have great upsert logic. Don't want to write a big gross update statement
    # So, I'll just completely delete and reinsert
    cursor.sql(f"DELETE FROM raw.team_goals_added WHERE season_name={season_name}")
    print("[teams_etl] Deleted data from team_goals_added")

    # manually add the season_name variable
    team_ga_data.insert(2, "season_name", season_name)


for i in tqdm(
    range(len(team_ga_data)), "Unnesting and inserting data for team goals added"
):
    row = team_ga_data.iloc[i]
    team_id = row["team_id"]
    minutes = row["minutes"]
    struct_value = row["data"]

    for value in struct_value:
        insert_stmt = f"""
        INSERT INTO raw.team_goals_added
        (team_id, minutes, season_name, data)
        VALUES
        ('{team_id}', {minutes}, {season_name}, {value})
        """

        cursor.sql(insert_stmt)
print("[teams_etl] Inserted data for team ga")


### team salary data

team_salary_data_present = check_for_existing_data(
    schema="raw", table_name="team_salaries", cursor=cursor
)

team_salary_data = asa_client.get_team_salaries(
    leagues="mls", split_by_teams=True, split_by_seasons=True
)

if team_salary_data_present and len(team_salary_data) > 0:
    cursor.sql("DELETE FROM kicking_dev.raw.team_salaries")
    print("[teams_etl] Deleted data for team_salaries")


cursor.sql("INSERT INTO raw.team_salaries SELECT * FROM team_salary_data")
print(f"[teams_etl] Inserted {len(team_salary_data):,} rows into salary table")


# team xg data
team_xg_data_present = check_for_existing_data(
    schema="raw", table_name="team_xg", cursor=cursor
)

team_xg_data = asa_client.get_team_xgoals(leagues="mls", season_name=season_name)


if team_xg_data_present and len(team_xg_data) > 0:
    cursor.sql(f"DELETE FROM kicking_dev.raw.team_xg WHERE season_name={season_name}")
    print("[teams_etl] Deleted data for team_xg")

team_xg_data.insert(1, "season_name", season_name)
cursor.sql("INSERT INTO raw.team_xg SELECT * FROM team_xg_data")
print(f"[teams_etl] Inserted {len(team_xg_data):,} rows into team xg table")

# team xpass data

team_xpass_data_present = check_for_existing_data(
    schema="raw", table_name="team_xpass", cursor=cursor
)

team_xpass_data = asa_client.get_team_xpass(leagues="mls", season_name=season_name)


if team_xpass_data_present and len(team_xpass_data) > 0:
    cursor.sql("DELETE FROM kicking_dev.raw.team_xpass")
    print("[teams_etl] Deleted data from team_xpass table ")

team_xpass_data.insert(1, "season_name", season_name)
cursor.sql("INSERT INTO raw.team_xpass SELECT * FROM team_xpass_data")
print(f"[teams_etl] Inserted {len(team_xpass_data):,} rows into team_xpass table")


# manager data

manager_data_present = check_for_existing_data(
    schema="raw", table_name="managers", cursor=cursor
)

manager_data = asa_client.get_managers(leagues="mls")

if manager_data_present:

    num_matching_keys, delete_stmt = delete_matching_pkeys_stmt(
        source_df=manager_data,
        target_table="kicking_dev.raw.managers",
        primary_key_column="manager_id",
    )
    cursor.sql(delete_stmt)
    print(f"[teams_etl] Deleted at least {num_matching_keys} from managers table")


cursor.sql("INSERT INTO raw.managers SELECT * FROM manager_data")
print(f"[teams_etl] Inserted {len(manager_data):,} rows into managers table")
