echo "Creating our virtual environment" &&
rm -rf .venv &&
python -m venv .venv &&
source .venv/Scripts/activate &&
echo "Installing dependencies" &&
pip install -q -r requirements.txt &&
echo "Beginning to fetch data" &&
python -u raw_data_etl/fetch_game_data.py &&
python -u raw_data_etl/fetch_team_data.py &&
python -u raw_data_etl/fetch_player_data.py &&
echo "Beginning to build our data model" &&
dbt build
$SHELL