import duckdb
import os


path_to_database = os.path.join(os.getcwd(), "data", "kicking_dev.db")
con = duckdb.connect(database=path_to_database, read_only=True)

x_value = "xgoals_for"

query = f"""
SELECT MIN({x_value})
FROM purty.aggregate_stats_with_points
"""

min_value = con.execute(query).fetchone()


query = f"""
SELECT MIN({x_value}), MAX({x_value})
FROM purty.aggregate_stats_with_points
"""

min_value, max_value = con.execute(query).fetchone()
