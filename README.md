# smart-store-gillespie

- author: Aaron Gillespie  
- date: 2025-03-20
- purpose: NW Missouri University CSIS 44-632 - BI & Analytics

# Setup

## Clone this repo to your machine

In a new VS Code window, click clone git repository:  

![alt text](assets/vsc_img.png)

Then paste the clone URL:  

`https://github.com/aarongilly/smart-store-gillespie.git`

## Initialize the virtual environment

The following commands establish a virtual python environment, activate it, and install the required packages from `requirements.txt`. This has become boilerplate to every project setup.

```shell
python3 -m venv .venv  
source .venv/bin/activate
python3 -m pip install --upgrade pip  
python3 -m pip install --upgrade -r requirements.txt
```

They can be executed one after the other, or all at once (where they'll be run in series anyway).

## Execute Setup Script

1. Activate the built-in VS Code terminal with shortcut key:  
    
    `ctrl + ~`

2. Verify the terminal utilzes the virtual environment (should say `.venv` somewhere).
3. Run the data prep script with the appropriate command:

    `python3 scripts/data_prep.py` <- Mac / Linux   
        -or-  
    `py scripts\data_prep.py` <- Windows PowerShell

## Testing

This project serves as our introduction to unit testing in Python. The `tests/` folder contains the following tests scripts.

### tests/test_data_scrubber.py

This test script consists of 13 checks which run against an internally-created temporary dataset. The dataset is created with known data quality issues. The `DataScrubber.py` script is invoked on the temporary dataset, creating a scrubbed DataFrame with a known expected output. The tester then flags any deviations between generated output and expected output.

## Database Documentation

The database in this project is designed to log _transactions_ and the necessary dimensions to add meaning to them. The table uses a snowflake schema, although it's small enough to nearly be star schmea.

![alt text](assets/schema.png)

The schema is as follows:

![alt text](assets/SCHEMA_TABLE.png)

### Design Choices
I went into this with the very simple rule - tables should be in 3rd normal form. This means No table contains columns whose value depends on other **non-primary-key** columns. This resulted in the creation ofthe `suppliers` table, turning what would have been a star schema into a light snowflake schema.

> [!warning] Challenges
> Several of my choices early on lead to headaches later

I added new tables that were not part of the minimum viable set. If I'd been satisfied with first normal form (which, for a data warehouse this size, is probably adequate), I could have saved myself a lot of headaches spinning up new tables. 

Even still, the data warehouse doesn't make a _ton_ of sense. I should probably re-title "TransactionID" to "SaleID" to keep the table name matches surrogate primary key theme consistent. The "state" of the Transaction has nothing to do with the store location. I justified this in retrospect by saying "I guess these are online orders, and the customer lives elsewhere" - but that's a stretch. You wouldn't allow this type of weird dependency in a real-world setting. My choice of adding "state" to the SALES data was arbitrary at the time, but left the data warehouse feeling strange.

> [!tldr] Lessons Learned
> - Schema changes are **tedious**
> - Keep it simple sometimes means accept some complexity

## Data Contents
The following sections display the first few rows & columns of each table.

### SALES
![alt text](assets/SALES.png)

### CUSTOMERS
![alt text](assets/CUSTOMERS.png)

### STORES
![alt text](assets/STORES.png)

### CAMPAIGNS
![alt text](assets/CAMPAIGNS.png)

### PRODUCTS
![alt text](assets/PRODUCTS.png)

### SUPPLIERS
![alt text](assets/SUPPLIERS.png)

# Spark & Juypter

Spark is a fast, distributed processing engine for large-scale data analysis. We used it with a Jupyter Notebook and Python by first configuring a SparkSession with JDBC connection details to your data warehouse. We then used Spark's DataFrame API (via PySpark) within the notebook to query, transform, and analyze tables from the data warehouse, visualizing insights directly in the notebook. This allows interactive exploration of (hypothetically) massive datasets without loading everything into memory. The jupyter notebook `spark_juypter_gillespie.ipynb` shows the basics of this process working in practice.

We 