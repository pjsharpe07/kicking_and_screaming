# kicking_and_screaming

[![Run Black Formatting check](https://github.com/pjsharpe07/kicking_and_screaming/actions/workflows/run_formatting.yml/badge.svg?branch=main)](https://github.com/pjsharpe07/kicking_and_screaming/actions/workflows/run_formatting.yml)

This is a repository that utilizes open MLS data provided by the American Soccer Analysis.

Find out more about them [from their website](https://www.americansocceranalysis.com/)


### Setup and development

As of writing, the code here will pull data from the opensource [itscalledsoccer](https://pypi.org/project/itscalledsoccer/) python package for the mls 2024 season.

It uses some different technologies:

- [python](https://www.python.org/) for fetching and loading the data. The needed packages can be found in the [requirements.txt](requirements.txt) file.
- [duckdb](https://duckdb.org/) for the database.
- [dbt](https://www.getdbt.com/) - this runs on dbt-core with the duckdb extension

The data model generally goes from schemas `raw` -> `intermediate` -> `purty`.

Raw is generated from the python scripts (details below) and intermediate & purty are generated by dbt.

#### Generating raw data

Raw is generated from the [run_all.py](./raw_data_etl/run_all.py) script kicks off the other [3 python scripts in the raw_data_etl folder](./raw_data_etl/).

Your easiest path is to run the [run_all.sh](./run_all.sh) which will run all the raw data and build downstream models with dbt. It will ask you for the mls year that you want to pull.

This loads data into the `raw` schema of your database. Most raw tables are completely replaced for all data in the year that was specified.

A sample python code to load player data would be below. Note that you should execute from the root of this repository. TODO: update to use absolute file paths instead of relative.

```
python raw_data_etl/fetch_player_data.py
```

It will prompt you for the year then loads some player data which you can then query like:

```
SELECT *
FROM kicking_dev.raw.players
```

#### Generating intermediate and purty

These are maintained by dbt. Assuming you have the correct installtions setup, you should just run `dbt build` which will build each step in the DAG. Then, you can verify some data by running something like...

```
SELECT *
FROM kicking_dev.purty.standings
```


#### Streamlit App

There is a basic data app that is built with [streamlit](https://streamlit.io/).

You can find the application code in [the app folder](./app/) and the config in the [.streamlit](.streamlit/) folder

Start the app from the root directory with `streamlit run app/app.py`.

### Contributing

Please make feature branches or PR's. Happy coding!

There is ony formatting check that runs using [black](https://pypi.org/project/black/)