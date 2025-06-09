# Financial Datawarehouse ETL

This project implements an ETL (Extract, Transform, Load) pipeline using SSIS to build a financial data warehouse. It automates the process of extracting transaction data from a source database, fetching historical exchange rates, and populating a data warehouse with enriched transaction details.

## Project Goal

- Extract all transaction dates from the source database.
- For each transaction date, fetch the corresponding exchange rates (USD, EUR, GBP, etc.) using the FRANKFURTER API.
- Create a data warehouse table containing:
  - Transaction details
  - Customer details
  - Supplier details
  - Transaction amounts in foreign currencies (USD, EUR, GBP)
  - Transaction amounts converted to USD

## Project Structure

- **Financial Datawarehouse ETL.sln**  
  Main SSIS solution file.

- **SSIS Project1/**  
  Contains SSIS packages and configurations for the ETL process.

- **data/**  
  Source data files or samples (if any).

- **data_flows/**  
  Data flow and control flow diagrams:
    - [CustomerTransactions_flow.png](https://github.com/dhaev/Data-projects/blob/main/SSIS/Financial%20Datawarehouse%20ETL/data_flows/CustomerTransactions_flow.png): Visualizes how customer transactions are processed.
    - [ExchangeRate_flow.png](https://github.com/dhaev/Data-projects/blob/main/SSIS/Financial%20Datawarehouse%20ETL/data_flows/ExchangeRate_flow.png): Illustrates fetching and applying exchange rates.
    - [Supplier_flow.png](https://github.com/dhaev/Data-projects/blob/main/SSIS/Financial%20Datawarehouse%20ETL/data_flows/Supplier_flow.png): Shows supplier data integration.
    - [control_flow.png](https://github.com/dhaev/Data-projects/blob/main/SSIS/Financial%20Datawarehouse%20ETL/data_flows/control_flow.png): Depicts the main control flow for the ETL orchestration.

## Key Features

- **Automated Exchange Rate Retrieval:**  
  Integrates with the FRANKFURTER API to fetch historical rates for each transaction date.

- **Comprehensive Data Integration:**  
  Combines customer, supplier, and transaction data for a complete view.

- **Currency Conversion:**  
  Calculates and stores transaction amounts in both original and USD values.

## Deployment & Scheduling

- The project is deployed to a Microsoft SQL Server (MSSQL) instance.
- An automated SQL Server Agent job is created to execute and schedule the ETL process, enabling regular and hands-free data warehouse updates.

## Technologies Used

- SQL Server Integration Services (SSIS)
- SQL Server Data Tools (SSDT)
- Microsoft SQL Server
- FRANKFURTER Exchange Rate API

## Usage

1. Open `Financial Datawarehouse ETL.sln` in Visual Studio or SSDT.
2. Configure database connections and any required environment variables.
3. Deploy the SSIS packages to your SQL Server.
4. Set up a SQL Server Agent Job to schedule and automate the ETL process.
5. Execute the job or run packages manually to extract, transform, and load data into the warehouse.
