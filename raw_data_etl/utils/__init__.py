from duckdb import DuckDBPyConnection


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
    """

    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
