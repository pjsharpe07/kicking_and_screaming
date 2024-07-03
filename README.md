# kicking_and_screaming

This is a repository that utilizes open MLS data provided by the American Soccer Analysis.

Find out more about them [from their website](https://www.americansocceranalysis.com/)


### Setup and development

As of writing, the code here will pull data from the opensource [itscalledsoccer](https://pypi.org/project/itscalledsoccer/) python package for the mls 2024 season.

It uses some different technologies:

- [python](https://www.python.org/) for fetching and loading the data. The needed packages can be found in the [requirements.txt](./raw_data_etl/requirements.txt) file.
- [duckdb](https://duckdb.org/) for the database.


There are [3 python scripts](./raw_data_etl/) that will load raw data into a duckdb database. You can run them in any order. Each one will create a directory called `data` if it doesn't exist where a database called `kicking_dev.db` will be created.

This loads data into the `raw` schema of your database. Note that this will only load data into each table if the table doesn't already have data. Incremental updates will be released in future versions.

A sample python code to load player data would be below. Note that you should execute from the root of this repository. TODO: update to use absolute file paths instead of relative.

```
python raw_data_etl/fetch_player_data.py
```

This loads some player data which you can then query like:

```
SELECT *
FROM kicking_dev.raw.players
```

### Contributing

Please make feature branches or PR's. Happy coding!

There is ony formatting check that runs using [black](https://pypi.org/project/black/)