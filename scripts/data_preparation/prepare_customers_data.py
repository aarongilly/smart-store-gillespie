"""
Module 3: Data Preparation Script - Customers
File: scripts/data_preparation/prepare_customers_data.py

I pulled this logic out from the main `data_prep.py` for the purposes of generating
unique files for the assignment, which requires one file per table. Otherwise, this
would have been included in the main script.
"""

import data_prep as dp
from data_scrubber import DataScrubber

def main() -> None:
    """Main function for pre-processing customer data."""


    df_customers = dp.read_raw_data("customers_data.csv")

    df_customers.columns = df_customers.columns.str.strip()  # Clean column names
    df_customers = df_customers.drop_duplicates()            # Remove duplicates

    df_customers['Name'] = df_customers['Name'].str.strip()  # Trim whitespace from column values
    df_customers = df_customers.dropna(subset=['CustomerID', 'Name'])  # Drop rows missing critical info
    
    scrubber_customers = DataScrubber(df_customers)
    scrubber_customers.check_data_consistency_before_cleaning()
    scrubber_customers.inspect_data()
    
    df_customers = scrubber_customers.handle_missing_data(fill_value="N/A")
    df_customers = scrubber_customers.parse_dates_to_add_standard_datetime('JoinDate')
    # ADDED CHECKS

    # Remove customers with birth year outside of a reasonable range
    df_customers = scrubber_customers.filter_column_outliers('BirthYear', 1900, 2025)

    # Trim whitespace from 'ReferringCustomer' column using new method of DataScrubber
    df_customers = scrubber_customers.format_column_strings_only_trim('ReferringCustomer')

    # END ADDED CHECKS
    scrubber_customers.check_data_consistency_after_cleaning()

    dp.save_prepared_data(df_customers, "customers_data_prepared.csv")

if __name__ == "__main__":
    main()