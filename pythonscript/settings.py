from pathlib import Path
from loguru import logger

class Settings():
    basedir = Path.cwd()
    rawdir = Path("raw_data")
    processeddir = Path("processed_data")
    logdir = basedir / "logs"

    house_activity_columns = [
        "House_address",
        "House_lat",
        "House_lng",
        "Activity_company",
        "Activity_address",
        "Activity_lat",
        "Activity_lng",
        "Activity_type",
    ]

    Settings = Settings()
    logger.add("logfile.log")