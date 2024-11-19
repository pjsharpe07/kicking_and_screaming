import os


# This runs the game, team, then player data raw pulls


season_name = int(input("Which season do you want to pull data? "))
print(f"Pulling all mls data for the year {season_name}")


file_paths = [
    os.path.join("raw_data_etl", "fetch_game_data.py"),
    os.path.join("raw_data_etl", "fetch_team_data.py"),
    os.path.join("raw_data_etl", "fetch_player_data.py"),
]

# iterate through all the paths and execute them
for path in file_paths:
    with open(path) as file:
        exec(file.read())
