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
from utils import check_for_existing_data, create_schema_if_not_exists
from tqdm import tqdm


path_to_database = os.path.join(os.getcwd(), "data", "kicking_dev.db")

# make the data directory if it doesn't exist
os.makedirs(os.path.dirname(path_to_database), exist_ok=True)


cursor = duckdb.connect(path_to_database)

# create the schema if it doesn't exist
create_schema_if_not_exists(schema="raw", cursor=cursor)

# now create the raw tables
cursor.execute(teams_table)
print("Teams table created/exists")
cursor.execute(team_conference_table)
print("Conference table created/exists")
cursor.execute(team_goals_added_table)
print("Team Goals added table created/exists")
cursor.execute(team_salaries_table)
print("Team Salary table created/exists")
cursor.execute(team_xg_table)
print("Team XG data table created/exists")
cursor.execute(team_xpass_table)
print("Team Xpass table created/exists")
cursor.execute(manager_table)
print("Manager table created/exists")


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


if conf_data_present:
    # TODO
    print("TODO: Build merge conference data logic")
else:
    cursor.sql("INSERT INTO raw.team_conferences SELECT * FROM conference_df")
    print("Inserted data for conferences!")

# free up memory
del conference_df

# now start fetching the raw data from ASA

# generic team data

asa_client = AmericanSoccerAnalysis()

team_data_present = check_for_existing_data(
    schema="raw", table_name="teams", cursor=cursor
)


if team_data_present:
    # TODO
    print("Build merge logic for teams data")
else:
    team_data = asa_client.get_teams(leagues="mls")
    cursor.sql("INSERT INTO raw.teams SELECT * FROM team_data")
    print("Inserted data for teams!")

    del team_data

# team goals added -- we have to unnest the data for this one

team_ga_data_present = check_for_existing_data(
    schema="raw", table_name="team_goals_added", cursor=cursor
)

if team_ga_data_present:
    # TODO: build merge logic -- probs hard
    print("TODO: Build team g+ data merge")
else:

    team_ga_data = asa_client.get_team_goals_added(leagues="mls", season_name="2024")

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
            (team_id, minutes, data)
            VALUES
            ('{team_id}',{minutes},{value})
            """

            cursor.sql(insert_stmt)
    print("Inserted data for team ga")
    del team_ga_data

### team salary data

team_salary_data_present = check_for_existing_data(
    schema="raw", table_name="team_salaries", cursor=cursor
)

if team_salary_data_present:
    # TODO
    print("TODO BUILD SALARY MERGE LOGIC")
else:
    team_salary_data = asa_client.get_team_salaries(
        leagues="mls", split_by_teams=True, split_by_seasons=True
    )

    cursor.sql("INSERT INTO raw.team_salaries SELECT * FROM team_salary_data")
    print("Inserted salary data")
    del team_salary_data


# team xg data

team_xg_data_present = check_for_existing_data(
    schema="raw", table_name="team_xg", cursor=cursor
)

if team_xg_data_present:
    # TODO
    print("TODO: Build team xg merge data")
else:
    team_xg_data = asa_client.get_team_xgoals(leagues="mls", season_name="2024")
    cursor.sql("INSERT INTO raw.team_xg SELECT * FROM team_xg_data")
    del team_xg_data
    print("Inserted data for team xg")

# team xpass data

team_xpass_data_present = check_for_existing_data(
    schema="raw", table_name="team_xpass", cursor=cursor
)

if team_xpass_data_present:
    # TODO
    print("TODO: Build team xpass merge data")
else:
    team_xpass_data = asa_client.get_team_xpass(leagues="mls", season_name="2024")
    cursor.sql("INSERT INTO raw.team_xpass SELECT * FROM team_xpass_data")
    del team_xpass_data
    print("Inserted data for team xpass")


# manager data

manager_data_present = check_for_existing_data(
    schema="raw", table_name="managers", cursor=cursor
)

if manager_data_present:
    # TODO
    print("TODO: Build manager merge data")
else:
    manager_data = asa_client.get_managers(leagues="mls")
    cursor.sql("INSERT INTO raw.managers SELECT * FROM manager_data")
    del manager_data
    print("Inserted data for managers")
