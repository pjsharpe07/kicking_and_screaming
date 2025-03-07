# 1. instantiate venv and install any changed dependencies
echo "Creating our virtual environment" &&
source .venv/Scripts/activate &&
echo "Installing dependencies" &&
python -m pip install --upgrade pip &&
pip install -q  -r requirements.txt --upgrade &&

# 2. fetch data from asa apis 
echo "Beginning to fetch data" &&
python -u raw_data_etl/run_all.py &&

# 3. build our models via dbt
echo "Beginning to build our data model" &&
dbt build
$SHELL