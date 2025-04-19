import pandas as pd
import sqlite3
import pathlib
import sys

# For local imports, temporarily add project root to sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Constants
DW_DIR = pathlib.Path("data/").joinpath("dw")
DB_PATH = DW_DIR.joinpath("smart_sales.db")
PREPARED_DATA_DIR = pathlib.Path("data").joinpath("prepared")

def create_schema(cursor: sqlite3.Cursor) -> None:
    """Create tables in the data warehouse if they don't exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            CustomerID INTEGER PRIMARY KEY,
            Name TEXT,
            Region TEXT,
            JoinDate TEXT,
            StandardJoinDate TEXT,
            ReferringCustomer INTEGER,
            Birthday TEXT,
            FOREIGN KEY (ReferringCustomer) REFERENCES customer (CustomerID)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            ProductID INTEGER PRIMARY KEY,
            ProductName TEXT,
            Category TEXT,
            UnitPrice REAL,
            Supplier INTEGER,
            RemainingInventory INTEGER,
            FOREIGN KEY (Supplier) REFERENCES suppliers (SupplierID)
        )
    """)
     
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            TransactionID INTEGER PRIMARY KEY,
            CustomerID INTEGER,
            ProductID INTEGER,
            StoreID INTEGER,
            CampaignID INTEGER,
            SaleAmount REAL,
            SaleDate TEXT,
            State TEXT,
            Discount REAL,
            StateCode TEXT,
            FOREIGN KEY (CustomerID) REFERENCES customers (CustomerID),
            FOREIGN KEY (ProductID) REFERENCES products (ProductID),
            FOREIGN KEY (StoreID) REFERENCES stores (StoreID),
            FOREIGN KEY (CampaignID) REFERENCES campaigns (CampaignID)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stores (
            StoreID INTEGER PRIMARY KEY,
            StoreName TEXT,
            StoreLocation TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS campaigns (
            CampaignID INTEGER PRIMARY KEY,
            CampaignName TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS suppliers (
            SupplierID INTEGER PRIMARY KEY,
            SupplierName TEXT
        )
    """)


def delete_existing_records(cursor: sqlite3.Cursor) -> None:
    """Delete all existing records from all tables."""
    cursor.execute("DELETE FROM customers")
    cursor.execute("DELETE FROM products")
    cursor.execute("DELETE FROM sales")
    cursor.execute("DELETE FROM suppliers")
    cursor.execute("DELETE FROM stores")
    cursor.execute("DELETE FROM campaigns")

def insert_to_table(df: pd.DataFrame, tablename: str, cursor: sqlite3.Cursor) -> None:
    """Insert sales data into the sales table."""
    df.to_sql(tablename, cursor.connection, if_exists="append", index=False)

def load_data_to_db() -> None:
    try:
        # Connect to SQLite – will create the file if it doesn't exist
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Create schema and clear existing records
        create_schema(cursor)
        delete_existing_records(cursor)

        # Load prepared data using pandas
        customers_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("customers_data_prepared.csv"))
        products_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("products_data_prepared.csv"))
        sales_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("sales_data_prepared.csv"))
        suppliers_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("suppliers_data_prepared.csv"))
        stores_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("stores_data_prepared.csv"))
        campaigns_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("campaigns_data_prepared.csv"))

        # Insert data into the database
        insert_to_table(customers_df, 'customers', cursor)
        insert_to_table(products_df, 'products', cursor)
        insert_to_table(sales_df, 'sales', cursor)
        insert_to_table(suppliers_df, 'suppliers', cursor)
        insert_to_table(stores_df, 'stores', cursor)
        insert_to_table(campaigns_df, 'campaigns', cursor)

        conn.commit()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    load_data_to_db()