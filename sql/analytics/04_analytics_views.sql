/*
Project: E-commerce Performance Analytics
Tech Stack: MySQL · Python · Power BI

File: 04_analytics_views.sql
Purpose:
- Create analytics-ready views for Power BI
- Centralize business logic in SQL (clean BI layer)
- Provide KPI tables at daily and monthly grain

*/

USE ops_portfolio;

-- ============================================================
-- View 1: Daily order KPIs (orders, revenue, AOV)
-- ============================================================
CREATE OR REPLACE VIEW vw_kpi_daily AS
SELECT
    d.date_value,
    d.year,
    d.month,
    d.month_name,
    COUNT(o.order_id) AS orders,
    SUM(o.total_amount) AS revenue,
    AVG(o.total_amount) AS aov
FROM fact_order o
JOIN dim_date d
    ON d.date_key = o.date_key
GROUP BY
    d.date_value, d.year, d.month, d.month_name;

-- ============================================================
-- View 2: Monthly order KPIs (great for executive dashboard)
-- ============================================================
CREATE OR REPLACE VIEW vw_kpi_monthly AS
SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(o.order_id) AS orders,
    SUM(o.total_amount) AS revenue,
    AVG(o.total_amount) AS aov
FROM fact_order o
JOIN dim_date d
    ON d.date_key = o.date_key
GROUP BY
    d.year, d.month, d.month_name
ORDER BY
    d.year, d.month;

-- ============================================================
-- View 3: Product performance (units + revenue)
-- If your fact_order_item does not have unit_price,
-- replace (quantity * unit_price) with line_total if you have it.
-- ============================================================
CREATE OR REPLACE VIEW vw_product_performance AS
SELECT
    p.product_id,
    p.product_name,
    SUM(oi.quantity) AS units_sold,
    SUM(oi.quantity * oi.unit_price) AS revenue
FROM fact_order_item oi
JOIN dim_product p
    ON p.product_id = oi.product_id
GROUP BY
    p.product_id, p.product_name;

-- ============================================================
-- View 4: Top 20 products by revenue (simple Power BI table)
-- ============================================================
CREATE OR REPLACE VIEW vw_product_top20 AS
SELECT *
FROM vw_product_performance
ORDER BY revenue DESC
LIMIT 20;

-- ============================================================
-- View 5: Customer value (orders, total spend, AOV)
-- ============================================================
CREATE OR REPLACE VIEW vw_customer_value AS
SELECT
    c.customer_id,
    c.customer_name,
    COUNT(o.order_id) AS orders,
    SUM(o.total_amount) AS total_spend,
    AVG(o.total_amount) AS aov
FROM fact_order o
JOIN dim_customer c
    ON c.customer_id = o.customer_id
GROUP BY
    c.customer_id, c.customer_name;

-- ============================================================
-- View 6: Monthly MAU (monthly active users) from logins
-- ============================================================
CREATE OR REPLACE VIEW vw_mau_monthly AS
SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(DISTINCT l.customer_id) AS mau
FROM fact_login l
JOIN dim_date d
    ON d.date_key = l.date_key
GROUP BY
    d.year, d.month, d.month_name
ORDER BY
    d.year, d.month;

-- ============================================================
-- View 7: Monthly buyers + login-to-buy conversion rate
-- ============================================================
CREATE OR REPLACE VIEW vw_login_to_buy_monthly AS
WITH logins AS (
    SELECT
        d.year,
        d.month,
        COUNT(DISTINCT l.customer_id) AS users_logged_in
    FROM fact_login l
    JOIN dim_date d
        ON d.date_key = l.date_key
    GROUP BY d.year, d.month
),
buyers AS (
    SELECT
        d.year,
        d.month,
        COUNT(DISTINCT o.customer_id) AS buyers
    FROM fact_order o
    JOIN dim_date d
        ON d.date_key = o.date_key
    GROUP BY d.year, d.month
)
SELECT
    lg.year,
    lg.month,
    lg.users_logged_in,
    COALESCE(byr.buyers, 0) AS buyers,
    (COALESCE(byr.buyers, 0) / NULLIF(lg.users_logged_in, 0)) AS login_to_buy_rate
FROM logins lg
LEFT JOIN buyers byr
    ON byr.year = lg.year AND byr.month = lg.month
ORDER BY lg.year, lg.month;
