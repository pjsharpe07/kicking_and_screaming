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

if game_data_present:

    missing_games = filter_values_from_df(
        source_table="kicking_dev.raw.games",
        target_df=game_data,
        checking_column="game_id",
        cursor=cursor,
    )

    cursor.sql("INSERT INTO raw.games SELECT * FROM missing_games")
    print(f"[games_etl] Inserted {len(missing_games)} new rows into games data")
else:
    cursor.sql("INSERT INTO raw.games SELECT * FROM game_data")
    print(f"[games_etl] Inserted {len(game_data)} rows into game data")


### get_game_xgoals
game_xg_data_present = check_for_existing_data(
    schema="raw", table_name="games", cursor=cursor
)

# filter game ids if we already have it
# note that if game_ids is null, then it will return ALL games
if game_xg_data_present:
    game_ids = filter_by_missing_field(
        source_table="kicking_dev.raw.games",
        target_table="kicking_dev.raw.game_xg",
        checking_column="game_id",
        cursor=cursor,
    )
    if len(game_ids) > 0:
        game_xg_data = asa_client.get_game_xgoals(leagues="mls", game_ids=game_ids)
        cursor.sql("INSERT INTO raw.game_xg SELECT * FROM game_xg_data")
        print(f"[games_etl] Inserted {len(game_xg_data)} rows into game_xg table")
    else:
        print(f"[games_etl] Inserted {len(game_ids)} rows into game_xg table")
else:
    game_xg_data = asa_client.get_game_xgoals(leagues="mls", game_ids=game_ids)
    game_ids = list(game_data["game_id"].unique())


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
    print(f"[games_etl] Inserted {len(stadia_data)} rows for stadiums")
