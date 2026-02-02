/*
Project: E-commerce Performance Analytics
Tech Stack: MySQL · Python · Power BI

File: 03-dim_date_table.sql
Purpose:
- Generate a comprehensive date dimension table
- Support time-based analysis (daily, monthly, yearly)
- Enable trend analysis, seasonality, and time intelligence in Power BI

Notes:
- Date dimension follows analytics best practices
- Used by all fact tables via date_key foreign keys

*/

USE ops_portfolio;

-- Build 400 days back to today
WITH RECURSIVE seq AS (
  SELECT CURDATE() - INTERVAL 399 DAY AS d
  UNION ALL
  SELECT d + INTERVAL 1 DAY FROM seq WHERE d < CURDATE()
)
INSERT INTO dim_date (date_key, date_value, year, month, month_name, week_of_year, day_of_week, day_name)
SELECT
  (YEAR(d)*10000 + MONTH(d)*100 + DAY(d)) AS date_key,
  d AS date_value,
  YEAR(d) AS year,
  MONTH(d) AS month,
  DATE_FORMAT(d, '%M') AS month_name,
  WEEK(d, 3) AS week_of_year,
  WEEKDAY(d) + 1 AS day_of_week,
  DATE_FORMAT(d, '%W') AS day_name
FROM seq;
