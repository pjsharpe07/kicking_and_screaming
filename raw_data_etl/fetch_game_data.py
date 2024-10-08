import duckdb
from itscalledsoccer.client import AmericanSoccerAnalysis
import os
from schemas_and_starting_scripts.raw_games_tables import (
    game_table,
    game_xg_table,
    stadium_table,
)
from utils import (
    check_for_existing_data,
    create_schema_if_not_exists,
    filter_by_missing_field,
    filter_values_from_df,
)

# suppress warnings -- pandas deprecation warning
# Remove once newer version of itscalledsoccer is released
import warnings

warnings.filterwarnings("ignore")


path_to_database = os.path.join(os.getcwd(), "data", "kicking_dev.db")

# make the data directory if it doesn't exist
os.makedirs(os.path.dirname(path_to_database), exist_ok=True)


cursor = duckdb.connect(path_to_database)

# create the schema if it doesn't exist
create_schema_if_not_exists(schema="raw", cursor=cursor)

# now create the raw tables
cursor.execute(game_table)
print("[games_etl] games table created/exists")
cursor.execute(game_xg_table)
print("[games_etl] xg table created")
cursor.execute(stadium_table)
print("[games_etl] data created")

# fetch raw data from asa

asa_client = AmericanSoccerAnalysis()


#### games

game_data_present = check_for_existing_data(
    schema="raw", table_name="games", cursor=cursor
)

game_data = asa_client.get_games(leagues="mls", seasons="2024")

if game_data_present and len(game_data) > 0:

    ##  apparently the upstream data from the api gets reinstated?
    ##  so we can't do this
    # missing_games = filter_values_from_df(
    #     source_table="kicking_dev.raw.games",
    #     target_df=game_data,
    #     checking_column="game_id",
    #     cursor=cursor,
    # )

    cursor.sql("DELETE FROM raw.games")
    print(f"[games_etl] Deleted raw games data")

cursor.sql("INSERT INTO raw.games SELECT * FROM game_data")
print(f"[games_etl] Inserted {len(game_data):,} rows into game data")


### get_game_xgoals
game_xg_data_present = check_for_existing_data(
    schema="raw", table_name="games", cursor=cursor
)

# filter game ids if we already have it
# note that if game_ids is null, then it will return ALL games

game_xg_data = asa_client.get_game_xgoals(leagues="mls", season_name="2024")

if game_xg_data_present and len(game_xg_data) > 0:

    #### apparently upstream data gets re
    # game_ids = filter_by_missing_field(
    #     source_table="kicking_dev.raw.games",
    #     target_table="kicking_dev.raw.game_xg",
    #     checking_column="game_id",
    #     cursor=cursor,
    # )

    cursor.sql("DELETE FROM raw.game_xg")
    print("[games_etl] Deleted data from raw.game_xg")

cursor.sql("INSERT INTO raw.game_xg SELECT * FROM game_xg_data")
print(f"[games_etl] Inserted {len(game_xg_data):,} rows into game_xg table")


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
    cursor.sql("INSERT INTO raw.stadiums SELECT * FROM stadia_data")
    print(f"[games_etl] Inserted {len(stadia_data):,} rows for stadiums")
