import os
from config import get_etl_script_path


# This runs the game, team, then player data raw pulls


season_name = int(input("Which season do you want to pull data? "))
print(f"Pulling all mls data for the year {season_name}")


file_paths = [
    get_etl_script_path("fetch_game_data.py"),
    get_etl_script_path("fetch_team_data.py"),
    get_etl_script_path("fetch_player_data.py"),
]

# iterate through all the paths and execute them
for path in file_paths:
    with open(path) as file:
        exec(file.read())
