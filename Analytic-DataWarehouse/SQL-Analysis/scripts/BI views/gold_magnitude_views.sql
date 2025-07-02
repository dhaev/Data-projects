-- View: gold.total_customers_by_country
CREATE OR ALTER VIEW gold.total_customers_by_country AS
SELECT
    country,
    COUNT(customer_key) AS total_customers
FROM gold.dim_customers dc
GROUP BY country;
GO

-- View: gold.total_customers_by_gender
CREATE OR ALTER VIEW gold.total_customers_by_gender AS
SELECT
    gender,
    COUNT(customer_key) AS total_customers
FROM gold.dim_customers dc
GROUP BY gender;
GO

-- View: gold.total_products_by_category
CREATE OR ALTER VIEW gold.total_products_by_category AS
SELECT
    category,
    COUNT(product_key) AS total_products
FROM gold.dim_products dp
WHERE category IS NOT NULL AND category <> 'n/a'
GROUP BY category;
GO

-- View: gold.avg_cost_by_category
CREATE OR ALTER VIEW gold.avg_cost_by_category AS
SELECT
    category,
    AVG(cost) AS avg_cost
FROM gold.dim_products dp
WHERE category IS NOT NULL AND category <> 'n/a'
GROUP BY category;
GO

-- View: gold.total_revenue_by_category
CREATE OR ALTER VIEW gold.total_revenue_by_category AS
SELECT
    dp.category,
    SUM(fs.sales_amount) AS total_revenue
FROM gold.fact_sales fs
LEFT JOIN gold.dim_products dp
    ON dp.product_key = fs.product_key
WHERE dp.category IS NOT NULL AND dp.category <> 'n/a'
GROUP BY dp.category;
GO

-- View: gold.total_revenue_by_customer
CREATE OR ALTER VIEW gold.total_revenue_by_customer AS
SELECT
    dc.customer_key,
    dc.first_name,
    dc.last_name,
    SUM(fs.sales_amount) AS total_revenue
FROM gold.fact_sales fs
LEFT JOIN gold.dim_customers dc
    ON dc.customer_key = fs.customer_key
GROUP BY 
    dc.customer_key,
    dc.first_name,
    dc.last_name;
GO

-- View: gold.total_sold_items_by_country
CREATE OR ALTER VIEW gold.total_sold_items_by_country AS
SELECT
    dc.country,
    SUM(fs.quantity) AS total_sold_items
FROM gold.fact_sales fs
LEFT JOIN gold.dim_customers dc
    ON dc.customer_key = fs.customer_key
GROUP BY dc.country;
GO

-- =============================================================================
-- Create Report: gold.report_key_metrics
-- =============================================================================
IF OBJECT_ID('gold.report_key_metrics', 'V') IS NOT NULL
    DROP VIEW gold.report_key_metrics;
GO

CREATE VIEW gold.report_key_metrics AS
SELECT 'Total Sales' AS metric, SUM(fs.sales_amount) AS value FROM gold.fact_sales fs
UNION ALL
SELECT 'Total Quantity', SUM(fs.quantity) FROM gold.fact_sales fs
UNION ALL
SELECT 'Average Price', AVG(fs.price) FROM gold.fact_sales fs
UNION ALL
SELECT 'Total Orders', COUNT(DISTINCT fs.order_number) FROM gold.fact_sales fs
UNION ALL
SELECT 'Total Products', COUNT(DISTINCT dp.product_name) FROM gold.dim_products dp
UNION ALL
SELECT 'Total Customers', COUNT(dc.customer_key) FROM gold.dim_customers dc;
GO
