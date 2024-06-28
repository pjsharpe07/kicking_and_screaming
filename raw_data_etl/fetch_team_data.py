import duckdb
from itscalledsoccer.client import AmericanSoccerAnalysis
from pandas import DataFrame
import os
from schemas_and_starting_scripts.raw_tables import teams_table, team_conference_table
from utils import check_for_existing_data, create_schema_if_not_exists




path_to_database = os.path.join(os.getcwd(), 'data', 'kicking_dev.db')

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
    print("PERFORM MERGE LOGIC TO DO")
else:
    cursor.sql("INSERT INTO raw.team_conferences SELECT * FROM conference_df")
    print("Inserted data for conferences!")

# free some memory -- even though this is a little guy
del conference_df

# now start fetching the raw data from ASA

asa_client = AmericanSoccerAnalysis()
team_data = asa_client.get_teams(leagues="mls")

data_present = check_for_existing_data(schema="raw", table_name="teams", cursor=cursor)


if data_present:
    # TODO
    print("PERFORM MERGE LOGIC")
else:
    cursor.sql("INSERT INTO raw.teams SELECT * FROM team_data")
    print("Inserted data for teams!")
