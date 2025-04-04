"""
Module 3: Data Preparation Script - Generic
File: scripts/data_preparation/prepare_generic_data.py

Adapted from the other "prepare_*.py" scripts, 
this script is designed to handle generic datasets
only doing the basic cleaning and preparation steps.
"""

import data_prep as dp
import pandas as pd
from data_scrubber import DataScrubber

def main(csv_name_without_extension: str) -> None:
    """Main function for pre-processing sales data."""

    df = dp.read_raw_data(csv_name_without_extension+'.csv')

    df.columns = df.columns.str.strip()  # Clean column names
    df = df.drop_duplicates()            # Remove duplicates
    
    scrubber_sales = DataScrubber(df)
    scrubber_sales.check_data_consistency_before_cleaning()
    scrubber_sales.inspect_data()
    
    df = scrubber_sales.handle_missing_data(fill_value="N/A")

    scrubber_sales.check_data_consistency_after_cleaning()

    dp.save_prepared_data(df, csv_name_without_extension +"_prepared.csv")

if __name__ == "__main__":
    main()
