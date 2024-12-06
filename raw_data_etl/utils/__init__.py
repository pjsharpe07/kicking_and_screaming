from duckdb import DuckDBPyConnection
from pandas import DataFrame


def check_for_existing_data(
    schema: str, table_name: str, cursor: DuckDBPyConnection
) -> bool:
    """
    Checks if there is existing data in the duckdb table

    Args:
        schema: The schema where the table exists
        table_name: The table name where the table exists
        cursor: The duckdb cursor to execute the query
    """
    table_count = cursor.execute(
        f"SELECT COUNT(*) FROM {schema}.{table_name}"
    ).fetchone()[0]
    return table_count > 0


def create_schema_if_not_exists(schema: str, cursor: DuckDBPyConnection) -> None:
    """
    Creates a schema if it does not exist in duckdb

    Args:
        schema: The name of the schema to create
        cursor: The duckdb cursor to execute the query
    """

    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def filter_by_missing_field(
    source_table: str,
    target_table: str,
    checking_column: str,
    cursor: DuckDBPyConnection,
) -> list:
    """
    Returns a list of the 'checking_column' from the db's source_table that is missing from the target_table

    Args:
        source_table: The table to where checking_column will be returned if not in target_table
        target_table: The table to filter checking_column
        checking_column: The column of values to return
        cursor: The duckdb cursor to execute the query
    """

    except_query = f"""
    SELECT {checking_column}
    FROM {source_table}
    EXCEPT
    SELECT {checking_column}
    FROM {target_table}
    """

    try:
        missing_values = [x[0] for x in cursor.execute(except_query).fetchall()]
        return missing_values
    except:
        raise ValueError(f"Unable to run:\n {except_query}")


def filter_values_from_df(
    source_table: str,
    target_df: DataFrame,
    checking_column: str,
    cursor: DuckDBPyConnection,
) -> DataFrame:
    """
    This returns the target_df filtered for only missing values in checking_column from the source_table

    Args:
        source_table: The name of the source table in the db
        target_df: The name of the target_df to filter and then return
        checking_column: The values to filter
        cursor: The duckdb cursor to execute the query
    """

    unique_source_values = [
        x[0]
        for x in cursor.sql(
            f"SELECT DISTINCT {checking_column} FROM {source_table}"
        ).fetchall()
    ]

    return target_df.loc[~target_df[checking_column].isin(unique_source_values)]


def delete_matching_pkeys_stmt(
    source_df: DataFrame, target_table: str, primary_key_column: str
) -> tuple:
    """
    Returns a delete statement from the target table
    where all rows are deleted where the primary_key_column in in the source_df
    """

    unique_values = source_df[primary_key_column].unique()
    in_clause = "', '".join(unique_values)

    delete_stmt = f"""
    DELETE FROM {target_table}
    WHERE {primary_key_column} IN ('{in_clause}')
    """
    return len(unique_values), delete_stmt


def validate_source_and_db_columns(
    source_df: DataFrame, target_table: str, cursor: DuckDBPyConnection
) -> DataFrame:
    """
    Identifies discrepancies in columns between source and target
    Prints warnings for differences.
    Extra columns in the source are dropped

    Args:
        source_df: the df returned from ASA api
        target_table: the string of the target table
        cursor: The instantiated cursor for duckdb
    """

    # first fetch the columns of both and turn into lists
    sql = f"DESCRIBE SELECT * FROM raw.{target_table}"
    db_columns = list(cursor.sql(sql).fetchdf()["column_name"].unique())

    source_columns = list(source_df.columns)

    # now identify and print any columns missing from source
    missing_from_source = [x for x in db_columns if x not in source_columns]
    if missing_from_source:
        print("!" * 150)
        print(
            f"Warning. The following columns are not found in source api when loading to {target_table} table."
        )
        print(", ".join(missing_from_source))
        print("!" * 150)

    # now identify and print any columns missing from db - warn AND drop
    missing_from_db = [x for x in source_columns if x not in db_columns]
    if missing_from_db:
        print("!" * 150)
        print(
            f"Warning! These columns are missing from {target_table} table and will be dropped before loading"
        )
        print(", ".join(missing_from_db))
        print("!" * 150)
        # drop them here
        source_df.drop(missing_from_db, axis=1, inplace=True)

    return source_df
