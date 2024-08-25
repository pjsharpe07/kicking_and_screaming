echo "Creating our virtual environment" &&
# 1. instantiate venv and install any changed dependencies
source .venv/Scripts/activate &&
echo "Installing dependencies" &&
pip install -q  -r requirements.txt --upgrade &&
# 2. fetch data from asa apis 
echo "Beginning to fetch data" &&
python -u raw_data_etl/fetch_game_data.py &&
python -u raw_data_etl/fetch_team_data.py &&
python -u raw_data_etl/fetch_player_data.py &&
# build our models via dbt
echo "Beginning to build our data model" &&
dbt build
$SHELL