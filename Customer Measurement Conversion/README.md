# Customer Measurement Conversion

This project demonstrates a simple ETL (Extract, Transform, Load) pipeline for processing customer measurement data from multiple file formats. The pipeline consolidates, transforms, and loads measurement data into both a CSV file and an SQLite database.

## Project Structure

- **etl_code.py**: Python script that orchestrates the entire ETL process:
  - Extracts data from CSV, JSON, and XML files.
  - Transforms measurements (converts height from inches to meters and weight from pounds to kilograms).
  - Loads transformed data into a new CSV file and an SQLite database.
  - Logs each step for traceability.
- **data/**: Contains source data files in various formats:
  - CSV: `source1.csv`, `source2.csv`, `source3.csv`
  - JSON: `source1.json`, `source2.json`, `source3.json`
  - XML: `source1.xml`, `source2.xml`, `source3.xml`
  - Also includes a ZIP archive with additional data.
- **M1_Instructions.pdf**: Project instructions and background (see PDF for details).

[View all data files here.](https://github.com/dhaev/Data-projects/tree/main/Customer%20Measurement%20Conversion/data)

## How it Works

1. **Extraction**: Reads all CSV, JSON, and XML files in the data directory.
2. **Transformation**: Converts height (inches → meters) and weight (pounds → kilograms), rounding to two decimals.
3. **Loading**: Writes the cleaned data to `transformed_data.csv` and to an SQLite database table.
4. **Logging**: Each ETL phase is logged to `log_file.txt`, including timestamps.

## Requirements

- Python 3.x
- pandas
- sqlite3

## Usage

1. Place your data files in the `data` directory.
2. Run `etl_code.py` to execute the ETL pipeline.
3. Check `transformed_data.csv`, the SQLite DB (`PEOPLE.db`), and `log_file.txt` for outputs.

## Notes

- Only the first few files in the `data` directory are shown above; [see all data files here](https://github.com/dhaev/Data-projects/tree/main/Customer%20Measurement%20Conversion/data).
- For more details, refer to the [M1_Instructions.pdf](https://github.com/dhaev/Data-projects/blob/main/Customer%20Measurement%20Conversion/M1_Instructions.pdf).
