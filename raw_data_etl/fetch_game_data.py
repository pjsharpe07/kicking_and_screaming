import duckdb
from itscalledsoccer.client import AmericanSoccerAnalysis
import os
from schemas_and_starting_scripts.raw_games_tables import (
    game_table,
    game_xg_table,
    stadium_table,
)
from utils import check_for_existing_data, create_schema_if_not_exists


path_to_database = os.path.join(os.getcwd(), "data", "kicking_dev.db")

# make the data directory if it doesn't exist
os.makedirs(os.path.dirname(path_to_database), exist_ok=True)


cursor = duckdb.connect(path_to_database)

# create the schema if it doesn't exist
create_schema_if_not_exists(schema="raw", cursor=cursor)

# now create the raw tables
cursor.execute(game_table)
print("games table created/exists")
cursor.execute(game_xg_table)
print("games xg table created")
cursor.execute(stadium_table)
print("stadium data created")

# fetch raw data from asa

asa_client = AmericanSoccerAnalysis()


#### games


game_data_present = check_for_existing_data(
    schema="raw", table_name="games", cursor=cursor
)

if game_data_present:
    # TODO -- fetch the max updated date from the table and filter to newer entries
    print("MERGE LOGIC HERE")
    game_data = asa_client.get_games(leagues="mls", seasons="2024")
else:
    game_data = asa_client.get_games(leagues="mls", seasons="2024")
    cursor.sql("INSERT INTO raw.games SELECT * FROM game_data")
    print("Inserted data for games")


### get_game_xgoals
# this takes a list of game ids and returns that data

game_xg_data_present = check_for_existing_data(
    schema="raw", table_name="games", cursor=cursor
)

# this can be removed once the above merge logic has been altered
if game_xg_data_present:
    # TODO
    print("Probs remove this")
else:
    game_ids = list(game_data["game_id"].unique())

    game_xg_data = asa_client.get_game_xgoals(leagues="mls", game_ids=game_ids)
    cursor.sql("INSERT INTO raw.game_xg SELECT * FROM game_xg_data")
    print("Inserted data for game xg")

### get_stadia

stadium_data_present = check_for_existing_data(
    schema="raw", table_name="stadiums", cursor=cursor
)

# this can be removed once the above merge logic has been altered
if stadium_data_present:
    # TODO
    print("Build merge logic")
else:
    stadia_data = asa_client.get_stadia(leagues="mls")
    cursor.sql("INSERT INTO raw.stadiums SELECT * FROM stadia_data")
    print("Inserted data for stadiums")
