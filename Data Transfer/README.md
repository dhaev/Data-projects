# Data Transfer

This subdirectory contains resources and scripts for transferring and synchronizing sales data between MySQL and PostgreSQL databases as part of a data warehouse pipeline. It is useful for ETL (Extract, Transform, Load) processes, database migration, or data warehousing demonstrations.

## Contents

- [`automation.py`](automation.py):  
  Python script to:
  - Connect to MySQL (staging) and PostgreSQL (production/data warehouse)
  - Retrieve the last processed row from PostgreSQL
  - Extract new records from MySQL `sales_data`
  - Load those records into PostgreSQL `sales_data`
  - Print summary statistics and handle connection errors

- [`sales.sql`](sales.sql):  
  SQL dump file for the MySQL `sales` database, including table schema and sample data for `sales_data`.

- [`sales.csv`](sales.csv):  
  CSV export of the sales data for reference, import, or analysis.

## How It Works

1. **automation.py** connects to both databases, finds the last inserted `rowid` in production, selects new records from staging, and inserts them into the production warehouse.
2. **sales.sql** sets up the MySQL staging database.
3. **sales.csv** provides a quick look or alternative import of the data.

## Requirements

- Python 3.x
- `mysql-connector-python`
- `psycopg2`
- Access to MySQL and PostgreSQL (or DB2) instances

## Usage

1. Import `sales.sql` into your MySQL database if needed.
2. Update credentials in `automation.py` for your environment.
3. Install dependencies: `pip install mysql-connector-python psycopg2`
4. Run the script: `python automation.py`
