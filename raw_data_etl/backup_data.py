import os
from datetime import datetime
from utils import copy_file_replace


today = datetime.now().strftime("%Y-%m-%d")

# set the path -- there is only one backup possible for the given date
path_to_database = os.path.join(os.getcwd(), "data", "kicking_dev.db")
path_to_backup = os.path.join(os.getcwd(), "data", "backup", f"kicking_dev_{today}.db")

# copy data
# will tell us if the backup was successful or not
copy_success = copy_file_replace(source = path_to_database, destination=path_to_backup)

if copy_success:
    print(f"Copied data from {path_to_database} to {path_to_backup}")
else:
    print(f"Unable to copy data. The source file does not exist")
