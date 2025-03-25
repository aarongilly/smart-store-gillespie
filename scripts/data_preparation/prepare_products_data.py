"""
Module 3: Data Preparation Script - Products
File: scripts/data_preparation/prepare_products_data.py

I pulled this logic out from the main `data_prep.py` for the purposes of generating
unique files for the assignment, which requires one file per table. Otherwise, this
would have been included in the main script.
"""

import data_prep as dp
from data_scrubber import DataScrubber

def main() -> None:
    """Main function for pre-processing product data."""


    df_products = dp.read_raw_data("products_data.csv")

    df_products.columns = df_products.columns.str.strip()  # Clean column names
    df_products = df_products.drop_duplicates()            # Remove duplicates

    df_products['ProductName'] = df_products['ProductName'].str.strip()  # Trim whitespace from column values
    df_products = df_products.dropna(subset=['ProductID', 'ProductName'])  # Drop rows missing critical info
    
    scrubber_products = DataScrubber(df_products)
    scrubber_products.check_data_consistency_before_cleaning()
    scrubber_products.inspect_data()
    
    df_products = scrubber_products.handle_missing_data(fill_value="N/A")
    scrubber_products.check_data_consistency_after_cleaning()

    dp.save_prepared_data(df_products, "products_data_prepared.csv")

if __name__ == "__main__":
    main()