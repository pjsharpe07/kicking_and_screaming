import duckdb
from itscalledsoccer.client import AmericanSoccerAnalysis
from config import path_to_database
from schemas_and_starting_scripts.raw_games_tables import (
    game_table,
    game_xg_table,
    stadium_table,
)
from utils import (
    check_for_existing_data,
    create_schema_if_not_exists,
    validate_source_and_db_columns,
)
import os

# make the data directory if it doesn't exist
os.makedirs(os.path.dirname(path_to_database), exist_ok=True)

# check for season name value
if "season_name" not in globals():
    season_name = int(input("Which season do you want to pull data? "))

print(f"[games_etl] Pulling game data for season: {season_name}")

cursor = duckdb.connect(path_to_database)

# create the schema if it doesn't exist
create_schema_if_not_exists(schema="raw", cursor=cursor)

# now create the raw tables
cursor.execute(game_table)
print("[games_etl] games table created/exists")
cursor.execute(game_xg_table)
print("[games_etl] xg table created")
cursor.execute(stadium_table)
print("[games_etl] stadium data created")

# fetch raw data from asa

asa_client = AmericanSoccerAnalysis()


#### games

game_data_present = check_for_existing_data(
    schema="raw", table_name="games", cursor=cursor
)

game_data = asa_client.get_games(leagues="mls", seasons=season_name)
game_data = validate_source_and_db_columns(
    source_df=game_data, target_table="games", cursor=cursor
)

if game_data_present and len(game_data) > 0:

    ##  apparently the upstream data from the api gets reinstated?
    ##  so we can't do this
    # missing_games = filter_values_from_df(
    #     source_table="kicking_dev.raw.games",
    #     target_df=game_data,
    #     checking_column="game_id",
    #     cursor=cursor,
    # )

    cursor.sql(f"DELETE FROM raw.games WHERE season_name = {season_name}")
    print(f"[games_etl] Deleted raw games data")

cursor.sql("INSERT INTO raw.games SELECT * FROM game_data")
print(f"[games_etl] Inserted {len(game_data):,} rows into game data")

del game_data

### get_game_xgoals
game_xg_data_present = check_for_existing_data(
    schema="raw", table_name="game_xg", cursor=cursor
)

# filter game ids if we already have it
# note that if game_ids is null, then it will return ALL games

game_xg_data = asa_client.get_game_xgoals(leagues="mls", season_name=season_name)

if game_xg_data_present and len(game_xg_data) > 0:

    #### apparently upstream data gets re
    # game_ids = filter_by_missing_field(
    #     source_table="kicking_dev.raw.games",
    #     target_table="kicking_dev.raw.game_xg",
    #     checking_column="game_id",
    #     cursor=cursor,
    # )

    # manually add the season name variable
    game_xg_data.insert(2, "season_name", season_name)
    game_xg_data = validate_source_and_db_columns(
        source_df=game_xg_data, target_table="game_xg", cursor=cursor
    )

    cursor.sql(f"DELETE FROM raw.game_xg WHERE season_name = {season_name}")
    print("[games_etl] Deleted data from raw.game_xg")

cursor.sql("INSERT INTO raw.game_xg SELECT * FROM game_xg_data")
print(f"[games_etl] Inserted {len(game_xg_data):,} rows into game_xg table")

del game_xg_data

### get_stadia

stadium_data_present = check_for_existing_data(
    schema="raw", table_name="stadiums", cursor=cursor
)

# this can be removed once the above merge logic has been altered
if stadium_data_present:
    # TODO
    print(
        "[games_etl] Build merge logic for stadia. Low priority -- how much does this even change...?"
    )
else:
    stadia_data = asa_client.get_stadia(leagues="mls")
    stadia_data = validate_source_and_db_columns(
        source_df=stadia_data, target_table="stadiums", cursor=cursor
    )
    cursor.sql("INSERT INTO raw.stadiums SELECT * FROM stadia_data")
    print(f"[games_etl] Inserted {len(stadia_data):,} rows for stadiums")

    del stadia_data
