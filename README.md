# E-commerce Performance Analytics
**MySQL · Python · Power BI**

## Project Overview
This project analyzes e-commerce performance using a relational data model built in MySQL.
The goal is to transform transactional data into actionable business insights related to
sales performance, customer behavior, and product trends.

## Tech Stack
- MySQL 8 (data modeling and transformations)
- SQL (CTEs, joins, aggregations, analytics views)
- Python (data generation and loading)
- Power BI (interactive dashboards and KPI reporting)

## Data Model
The database follows a star schema design:
- Fact tables: orders, order items, logins
- Dimension tables: date, customer, product

## Key Metrics
- Total orders and revenue
- Average order value
- Customer activity and engagement
- Product performance trends

## How to Run (MySQL)
1. Run `sql/setup/01_create_db_and_tables.sql`
2. Run `sql/setup/02_seed_dimensions.sql`
3. Run `sql/setup/03_build_dim_date.sql`
4. Load fact data using `python/generate_and_load.py`

## Power BI Report
The repository includes the Power BI `.pbix` file for model and measure inspection.
Live data refresh requires a local MySQL instance and is not expected to run out-of-the-box.

## ETL Pipeline
The Python script performs extraction, validation, and export of data from MySQL to analytics-ready formats (CSV and Parquet).


