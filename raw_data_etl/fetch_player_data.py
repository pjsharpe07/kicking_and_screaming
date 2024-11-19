import duckdb
from itscalledsoccer.client import AmericanSoccerAnalysis
import os
from utils import (
    check_for_existing_data,
    create_schema_if_not_exists,
    delete_matching_pkeys_stmt,
)
from schemas_and_starting_scripts.raw_player_tables import (
    player_table,
    player_ga_table,
    player_xg_table,
    player_xpass_table,
    player_salary_table,
    goalie_xg_table,
    goalie_ga_table,
)
from tqdm import tqdm

# check for season name value
if 'season_name' not in globals():
    season_name = int(input("Which season do you want to pull data? "))

print(f"[player_etl] Pulling player data for season: {season_name}")

path_to_database = os.path.join(os.getcwd(), "data", "kicking_dev.db")

# make the data directory if it doesn't exist
os.makedirs(os.path.dirname(path_to_database), exist_ok=True)


cursor = duckdb.connect(path_to_database)

# create the schema if it doesn't exist
create_schema_if_not_exists(schema="raw", cursor=cursor)

# now create the raw tables
cursor.execute(player_table)
print("[player_etl] player table created/exists")
cursor.execute(player_ga_table)
print("[player_etl] player ga table created/exists")
cursor.execute(player_xg_table)
print("[player_etl] player xg table created/exists")
cursor.execute(player_xpass_table)
print("[player_etl] player xpass table created/exists")
cursor.execute(player_salary_table)
print("[player_etl] player salary table created/exists")
cursor.execute(goalie_xg_table)
print("[player_etl] goalie xg table created/exists")
cursor.execute(goalie_ga_table)
print("[player_etl] goalie ga table created/exists")


asa_client = AmericanSoccerAnalysis()

## players

player_data_present = check_for_existing_data(
    schema="raw", table_name="players", cursor=cursor
)

player_data = asa_client.get_players(leagues="mls")


if player_data_present:
    num_matching_keys, delete_stmt = delete_matching_pkeys_stmt(
        source_df=player_data,
        target_table="kicking_dev.raw.players",
        primary_key_column="player_id",
    )
    cursor.sql(delete_stmt)
    print(f"[player_etl] Deleted at least {num_matching_keys} from players table")

cursor.sql("INSERT INTO raw.players SELECT * FROM player_data")
print(f"[player_etl] Inserted {len(player_data):,} rows into players table")


# players ga

player_ga_data_present = check_for_existing_data(
    schema="raw", table_name="player_goals_added", cursor=cursor
)

# we have to break this up into many requests - it times out the asa server if we don't
# we should be kind :)
action_types = [
    "Dribbling",
    "Fouling",
    "Interrupting",
    "Passing",
    "Receiving",
    "Shooting",
]

if player_ga_data_present:
    cursor.sql(f"DELETE FROM kicking_dev.raw.player_goals_added WHERE season_name = {season_name}")
    print("[player etl] Deleted records from raw.player_goals_added")

for action_type in action_types:

    player_ga_data = asa_client.get_player_goals_added(
        leagues="mls",
        season_name=season_name,
        split_by_games=True,
        action_type=action_type,
        minimum_minutes=427,  # ~1/4 of playable minutes thus far
    )

    # manually add the season name column
    player_ga_data.insert(1, "season_name", season_name)

    # now unnest the "data" column
    player_ga_data["data"] = player_ga_data["data"].apply(lambda x: x[0])

    # now insert
    cursor.sql("INSERT INTO raw.player_goals_added SELECT * FROM player_ga_data")
    print(
        f"[player_etl] Inserted {len(player_ga_data):,} rows into player GA data for {action_type}"
        )


# player xg

player_xg_data_present = check_for_existing_data(
    schema="raw", table_name="player_xg", cursor=cursor
)

if player_xg_data_present:
    cursor.sql(f"DELETE FROM kicking_dev.raw.player_xg WHERE season_name = {season_name}")
    print("[player_etl] Deleted records from raw.player_xg")

player_xg_data = asa_client.get_player_xgoals(
    leagues="mls", season_name=season_name, split_by_teams=True, split_by_games=True
)

# manually add the season name variable
player_xg_data.insert(1, "season_name", season_name)

cursor.sql("INSERT INTO raw.player_xg SELECT * FROM player_xg_data")
print(f"[player_etl] Inserted {len(player_xg_data):,} rows for player xg")


# player xpass

player_xpass_data_present = check_for_existing_data(
    schema="raw", table_name="player_xpass", cursor=cursor
)

if player_xpass_data_present:
    cursor.sql(f"DELETE FROM kicking_dev.raw.player_xpass WHERE season_name = {season_name}")
    print("[player_etl] Deleted from raw.player_xpass")

player_xpass_data = asa_client.get_player_xpass(
    leagues="mls", season_name="2024", split_by_games=True
)

player_xpass_data.insert(1, "season_name", season_name)

cursor.sql("INSERT INTO raw.player_xpass SELECT * FROM player_xpass_data")
print(f"[player_etl] Inserted {len(player_xpass_data):,} rows for player xpass")

# player salaries

player_salary_data_present = check_for_existing_data(
    schema="raw", table_name="player_salaries", cursor=cursor
)

if player_salary_data_present:
    # TODO -- fetch the max updated date from the table and filter to newer entries
    print(
        "TODO: player salary table merge logic here. Low priority since CY data is not present"
    )
else:
    salary_data = asa_client.get_player_salaries(leagues="mls")
    cursor.sql("INSERT INTO raw.player_salaries SELECT * FROM salary_data")
    print("Inserted data for player salary")


# goalie xg data

goalie_xg_data_present = check_for_existing_data(
    schema="raw", table_name="goalie_xg", cursor=cursor
)

if goalie_xg_data_present:
    cursor.sql(f"DELETE FROM kicking_dev.raw.goalie_xg WHERE season_name = {season_name}")
    print("[player_etl] Deleted records from raw.goalie_xg")

goalie_xg_data = asa_client.get_goalkeeper_xgoals(
    leagues="mls", season_name=season_name, split_by_games=True
)

goalie_xg_data.insert(1, "season_name", season_name)

cursor.sql("INSERT INTO raw.goalie_xg SELECT * FROM goalie_xg_data")
print(f"[player_etl] Inserted {len(goalie_xg_data):,} rows into goalie xg data")


# goalie ga data

goalie_ga_data_present = check_for_existing_data(
    schema="raw", table_name="goalie_goals_added", cursor=cursor
)

if goalie_ga_data_present:
    cursor.sql(f"DELETE FROM kicking_dev.raw.goalie_goals_added WHERE season_name = {season_name}")
    print("[player_etl] Deleted records from raw.goalie_g+")

goalie_ga_data_present = asa_client.get_goalkeeper_goals_added(
    leagues="mls", season_name="2024", split_by_games=True
)

for i in tqdm(range(len(goalie_ga_data_present)), "Inserting goalie ga data"):
    row = goalie_ga_data_present.iloc[i]

    player_id = row["player_id"]
    game_id = row["game_id"]
    team_id = row["team_id"]
    minutes_played = row["minutes_played"]
    struct_value = row["data"]

    for value in struct_value:
        insert_stmt = f"""
        INSERT INTO raw.goalie_goals_added
        (player_id, season_name, game_id, team_id, minutes_played, data)
        VALUES
        ('{player_id}', {season_name}, '{game_id}', '{team_id}', {minutes_played}, {value})
        """
        cursor.sql(insert_stmt)

print("Inserted data for goalie g+ data")
