import requests
import json
import datetime
import os
import csv
import sys
import logging
from logging.handlers import RotatingFileHandler
import pyodbc
import concurrent.futures # NEW: For concurrent execution

# --- Configuration ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXCHANGE_RATES_FOLDER = os.path.join(BASE_DIR, "exchange_rates")
CSV_OUTPUT_FILENAME = "exchange_rates.csv"

# --- Log File Configuration ---
LOG_FILENAME = "exchange_rates_etl.log"
LOG_FILE_PATH = os.path.join(BASE_DIR, LOG_FILENAME)
MAX_LOG_FILE_SIZE_MB = 5
BACKUP_COUNT = 2

# --- API Configuration (FOR FRANKFURTER) ---
BASE_API_URL = "https://api.frankfurter.dev/v1/"
DEFAULT_BASE_CURRENCY = "USD"

# --- Database Connection Configuration ---
# !!! IMPORTANT: REPLACE WITH YOUR ACTUAL SQL SERVER AND DATABASE DETAILS !!!
DB_CONFIG = {
    "server": "localhost\\MSSQLSERVER01",  # Example: "YOUR_SQL_SERVER_NAME" or "server_ip\\instance_name"
    "database": "financial_transactions_db", # Example: "YOUR_DATABASE_NAME"
    "driver": "{ODBC Driver 17 for SQL Server}" # Common driver; ensure it's installed
}

# --- Concurrency Configuration ---
# Adjust MAX_WORKERS based on your system resources and API rate limits.
# A common starting point is (number of CPU cores * 2) or slightly higher for I/O-bound tasks.
MAX_WORKERS = 10 # Example: Allows 10 concurrent API requests

# --- Logger Setup ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

file_handler = RotatingFileHandler(
    LOG_FILE_PATH,
    maxBytes=MAX_LOG_FILE_SIZE_MB * 1024 * 1024,
    backupCount=BACKUP_COUNT
)
file_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

def get_dates_from_database():
    """
    Connects to the SQL Server database and yields distinct transaction dates
    one by one in YYYY-MM-DD format, without storing all dates in memory.
    """
    conn_str = (
        f"DRIVER={DB_CONFIG['driver']};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        "Trusted_Connection=yes;" # Using Windows Authentication
        # If using SQL Server Authentication, use:
        # f"UID={DB_CONFIG['username']};"
        # f"PWD={DB_CONFIG['password']};"
    )
    
    try:
        logger.info(f"Attempting to connect to SQL Server: {DB_CONFIG['server']}/{DB_CONFIG['database']}")
        with pyodbc.connect(conn_str) as cnxn:
            cursor = cnxn.cursor()

            sql_query = """
            SELECT DISTINCT CONVERT(NVARCHAR(10), transaction_date, 120)
            FROM financial_transactions 
            WHERE transaction_date IS NOT NULL
            ORDER BY CONVERT(NVARCHAR(10), transaction_date, 120); 
            """
            
            logger.info(f"Executing SQL query to fetch dates.")
            cursor.execute(sql_query)
            
            for row in cursor:
                yield row[0] # Yield the date string
            
            logger.info("Finished yielding dates from the database.")

    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        logger.critical(f"Database connection or query failed: {sqlstate} - {ex.args[1]}")
        raise
    except Exception as e:
        logger.critical(f"An unexpected error occurred while fetching dates from DB: {e}")
        raise


def fetch_exchange_rate_for_date(target_date_str: str, base_currency: str = DEFAULT_BASE_CURRENCY):
    """
    Fetches exchange rate data for a specific date from the Frankfurter API history endpoint.
    target_date_str should be in YYYY-MM-DD format.
    Returns a tuple of (requested_date_str, raw_json_response)
    """
    try:
        date_obj = datetime.datetime.strptime(target_date_str, '%Y-%m-%d').date()
        formatted_date = date_obj.isoformat() # Ensures YYYY-MM-DD format
    except ValueError:
        logger.error(f"Invalid date format for API call: {target_date_str}. Expected YYYY-MM-DD.")
        return target_date_str, None # Return date for context

    url = f"{BASE_API_URL}{formatted_date}?from={base_currency}"
    # logger.info(f"Fetching data from: {url}") # Moved logging to submission

    try:
        response = requests.get(url)
        response.raise_for_status()
        return target_date_str, response.json() # Return date along with response
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch data for {target_date_str} from API: {e}")
        return target_date_str, None


def transform_json_to_csv_rows(raw_json_data: dict, requested_date: str) -> list[dict]:
    """
    Transforms a single raw JSON API response from Frankfurter into a list of flattened dictionaries (rows)
    suitable for CSV output. Each dictionary represents a single exchange rate.
    Uses the requested_date for the effective_date to maintain consistency for weekend dates.
    """
    transformed_rows = []
    if raw_json_data and raw_json_data.get("base"):
        base_currency = raw_json_data.get("base")
        effective_date = requested_date

        conversion_rates = raw_json_data.get("rates", {})

        for to_currency, rate in conversion_rates.items():
            try:
                transformed_rows.append({
                    "from_currency": base_currency,
                    "to_currency": to_currency,
                    "exchange_rate": float(rate),
                    "effective_date": effective_date
                })
            except (ValueError, TypeError):
                logger.warning(f"Skipping rate for {to_currency} due to invalid value: {rate}.")
                continue

    return transformed_rows


if __name__ == "__main__":
    # --- 0. Setup Output Directories ---
    os.makedirs(EXCHANGE_RATES_FOLDER, exist_ok=True)

    csv_output_path = os.path.join(BASE_DIR, CSV_OUTPUT_FILENAME)
    
    # Flag to ensure CSV header is written only once
    header_written = False
    dates_found = False # New flag to track if any dates were found

    try:
        # Open CSV file in write mode; DictWriter handles writing rows
        with open(csv_output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = None # Initialize writer; it will be set once fieldnames are known

            # --- NEW: Use ThreadPoolExecutor for concurrent API calls ---
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Submit tasks for each date to the thread pool
                future_to_date = {
                    executor.submit(fetch_exchange_rate_for_date, target_date_str.strip()): target_date_str.strip()
                    for target_date_str in get_dates_from_database()
                    if target_date_str.strip() # Ensure date string is not empty
                }
                
                if not future_to_date:
                    logger.critical("No dates were obtained from the database to process. Exiting.")
                    sys.exit(1)

                dates_found = True # At least one date was submitted

                # Process results as they complete
                for future in concurrent.futures.as_completed(future_to_date):
                    target_date_str = future_to_date[future]
                    try:
                        requested_date, raw_json_response = future.result() # Get the (date, response) tuple
                        logger.info(f"Received API response for {requested_date}.")

                        if raw_json_response:
                            # Optional: Save raw JSON response for debugging/archiving
                            json_output_filename = f"exchange_rate_{requested_date}.json"
                            json_output_path = os.path.join(EXCHANGE_RATES_FOLDER, json_output_filename)
                            try:
                                with open(json_output_path, 'w', encoding='utf-8') as json_file:
                                    json.dump(raw_json_response, json_file, indent=4)
                                logger.info(f"Successfully saved raw JSON for {requested_date} to: {json_output_path}")
                            except IOError as e:
                                logger.error(f"Failed to save raw JSON for {requested_date}: {e}")

                            # Transform data
                            transformed_rows = transform_json_to_csv_rows(raw_json_response, requested_date)
                            
                            if transformed_rows:
                                if not header_written:
                                    fieldnames = list(transformed_rows[0].keys())
                                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                                    writer.writeheader()
                                    header_written = True
                                    logger.info("CSV header written.")

                                writer.writerows(transformed_rows)
                                logger.info(f"Successfully written transformed data for {requested_date} to CSV.")
                            else:
                                logger.warning(f"No transformed data generated for {requested_date} even after successful API fetch.")
                        else:
                            logger.warning(f"Skipping CSV transformation for {requested_date} due to API fetch error or invalid response.")
                    except Exception as exc:
                        logger.error(f"'{target_date_str}' generated an exception: {exc}")

        # Final check if any data was written at all (i.e., if header was ever written or if no dates found)
        if not dates_found:
            logger.critical("No dates were obtained from the database to process. Exiting.")
            sys.exit(1)
        elif not header_written: # Dates were found, but no data was successfully transformed and written
            logger.error("Dates were found, but no transformed data was written to the combined CSV. Check API responses and transformation logic.")
            sys.exit(1)
        else:
            logger.info(f"All available data processed and written to {csv_output_path}")

    except pyodbc.Error: # Caught if get_dates_from_database fails to connect/query
        sys.exit(1)
    except Exception as e:
        logger.critical(f"An unexpected error occurred during the overall process: {e}")
        sys.exit(1)