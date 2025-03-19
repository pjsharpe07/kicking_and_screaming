import os
import sys
from pathlib import Path

# Get the absolute path to the project root directory
PROJECT_ROOT = Path(__file__).parent.parent.resolve()

# Define key paths as absolute paths
RAW_DATA_ETL_DIR = PROJECT_ROOT / "raw_data_etl"
DATA_DIR = PROJECT_ROOT / "data"
DATABASE_PATH = DATA_DIR / "kicking_dev.db"
path_to_database = str(DATABASE_PATH)

# Ensure the data directory exists
DATA_DIR.mkdir(exist_ok=True)


def get_etl_script_path(script_name: str) -> Path:
    """Get the absolute path for an ETL script."""
    return RAW_DATA_ETL_DIR / script_name


# Add the project root to Python path to enable absolute imports
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))
