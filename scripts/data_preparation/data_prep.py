"""
Module 3: Data Preparation Script
File: scripts/data_prep.py

> [!notice] Script is a modified version of the example.
> Most of this script is lifted from the example. However I 
> am using the DataScrubber class from the scripts/data_scrubber.py
> to do a few additional things for my particular added data.

It loads raw CSV files from the 'data/raw/' directory, cleans and prepares each file, 
and saves the prepared data to 'data/prepared/'.
The data preparation steps include removing duplicates, handling missing values, 
trimming whitespace, and more.

This script uses the general DataScrubber class and its methods to perform common, reusable tasks.

To run it, open a terminal in the root project folder.
Activate the local project virtual environment.
Choose the correct command for your OS to run this script.

py scripts\data_prep.py
python3 scripts\data_prep.py
"""

import pathlib
import sys
import pandas as pd
import prepare_customers_data
import prepare_products_data
import prepare_sales_data

# For local imports, temporarily add project root to Python sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent # 3 levels up
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Now we can import local modules
from utils.logger import logger 

# Constants
DATA_DIR: pathlib.Path = PROJECT_ROOT.joinpath("data")
RAW_DATA_DIR: pathlib.Path = DATA_DIR.joinpath("raw")
PREPARED_DATA_DIR: pathlib.Path = DATA_DIR.joinpath("prepared")

def read_raw_data(file_name: str) -> pd.DataFrame:
    """Read raw data from CSV."""
    file_path: pathlib.Path = RAW_DATA_DIR.joinpath(file_name)
    return pd.read_csv(file_path)

def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    """Save cleaned data to CSV."""
    file_path: pathlib.Path = PREPARED_DATA_DIR.joinpath(file_name)
    df.to_csv(file_path, index=False)
    logger.info(f"Data saved to {file_path}")

def main() -> None:
    """Main function for pre-processing customer, product, and sales data."""
    logger.info("======================")
    logger.info("STARTING data_prep.py")
    logger.info("======================")


    logger.info("========================")
    logger.info("Starting CUSTOMERS prep")
    logger.info("========================")
    prepare_customers_data.main()

    logger.info("========================")
    logger.info("Starting PRODUCTS prep")
    logger.info("========================")
    prepare_products_data.main()

    logger.info("========================")
    logger.info("Starting SALES prep")
    logger.info("========================")
    prepare_sales_data.main()

    logger.info("======================")
    logger.info("FINISHED data_prep.py")
    logger.info("======================")

if __name__ == "__main__":
    main()