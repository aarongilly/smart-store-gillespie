"""
Module 3: Data Preparation Script - Sales
File: scripts/data_preparation/prepare_sales_data.py

I pulled this logic out from the main `data_prep.py` for the purposes of generating
unique files for the assignment, which requires one file per table. Otherwise, this
would have been included in the main script.
"""

import data_prep as dp
import pandas as pd
from data_scrubber import DataScrubber

def main() -> None:
    """Main function for pre-processing sales data."""

    df_sales = dp.read_raw_data("sales_data.csv")

    df_sales.columns = df_sales.columns.str.strip()  # Clean column names
    df_sales = df_sales.drop_duplicates()            # Remove duplicates

    df_sales['SaleDate'] = pd.to_datetime(df_sales['SaleDate'], errors='coerce')  # Ensure sale_date is datetime
    df_sales = df_sales.dropna(subset=['CustomerID', 'TransactionID', 'ProductID', 'SaleDate'])  # Drop rows missing critical info
    
    scrubber_sales = DataScrubber(df_sales)
    scrubber_sales.check_data_consistency_before_cleaning()
    scrubber_sales.inspect_data()
    
    df_sales = scrubber_sales.handle_missing_data(fill_value="N/A")

    # ADDED CHECKS
    df_sales = scrubber_sales.add_state_code_column('State')

    scrubber_sales.check_data_consistency_after_cleaning()

    dp.save_prepared_data(df_sales, "sales_data_prepared.csv")

if __name__ == "__main__":
    main()
