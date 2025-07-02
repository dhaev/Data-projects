/*
===============================================================================
Advanced Analytics for Visual Report
===============================================================================
Purpose:
    - To provide advanced analytics for product sales trends and segmentation.
    - To identify top-performing products by quarter and month.
    - To analyze yearly product and category sales for reporting and visualization.
    - To analyze sales and product trends by customer age group.

Highlights:
    1. Uses window functions for ranking and aggregating product sales.
    2. Identifies the most sought-after items in each quarter and month.
    3. Provides yearly analysis for both products and categories.
    4. Analyzes sales and product trends by age group.
    5. Designed for integration with BI dashboards and visual reports.
===============================================================================
*/

-- =============================================================================
-- Product Sales Analysis by Quarter and Month
-- =============================================================================
WITH ProductMonthlyQuarterlyAgg AS (
    -- Step 1: Calculate the base aggregated quantity for each product per month and quarter.
    SELECT
        DATEPART(quarter, f.order_date) AS OrderQuarter,
        MONTH(f.order_date) AS OrderMonth,
        p.product_name AS ProductName,
        SUM(f.quantity) AS ProductTotalQuantityInMonthQuarter
    FROM
        gold.fact_sales f
    LEFT JOIN
        gold.dim_products p ON f.product_key = p.product_key
    WHERE
        f.order_date IS NOT NULL
    GROUP BY
        DATEPART(quarter, f.order_date),
        MONTH(f.order_date),
        p.product_name
),
CalculatedMetrics AS (
    -- Step 2: Apply window functions to calculate totals and ranks.
    SELECT
        pma.OrderQuarter,
        pma.OrderMonth,
        pma.ProductName,
        pma.ProductTotalQuantityInMonthQuarter,
        SUM(pma.ProductTotalQuantityInMonthQuarter) OVER (
            PARTITION BY pma.OrderQuarter, pma.ProductName
        ) AS ProductTotalQuantityInQuarter,
        SUM(pma.ProductTotalQuantityInMonthQuarter) OVER (
            PARTITION BY pma.OrderMonth, pma.ProductName
        ) AS ProductTotalQuantityInMonth,
        SUM(pma.ProductTotalQuantityInMonthQuarter) OVER (
            PARTITION BY pma.OrderQuarter
        ) AS QuarterGrandTotalQuantity,
        DENSE_RANK() OVER (
            PARTITION BY pma.OrderQuarter
            ORDER BY pma.ProductTotalQuantityInMonthQuarter DESC
        ) AS ProductRankInQuarter
    FROM
        ProductMonthlyQuarterlyAgg pma
)
SELECT
    cm.OrderQuarter,
    cm.OrderMonth,
    cm.ProductName,
    cm.ProductTotalQuantityInMonthQuarter,
    cm.ProductTotalQuantityInQuarter,
    cm.ProductTotalQuantityInMonth,
    cm.QuarterGrandTotalQuantity,
    cm.ProductRankInQuarter,
    DENSE_RANK() OVER (ORDER BY cm.QuarterGrandTotalQuantity DESC) AS QuarterOverallRank
FROM
    CalculatedMetrics cm
WHERE
    cm.ProductRankInQuarter <= 3; -- Top 3 products per quarter
GO

-- =============================================================================
-- Product Analysis by Year
-- =============================================================================
CREATE VIEW gold.product_analysis_by_year AS
WITH yearly_summary_of_products AS (
    SELECT
        YEAR(order_date) AS order_year,
        product_key,
        SUM(sales_amount) AS YearlyProductSale
    FROM
        gold.fact_sales
    WHERE
        YEAR(order_date) IS NOT NULL
    GROUP BY
        YEAR(order_date),
        product_key
),
total_sales_for_each_product AS (
    SELECT
        y.order_year,
        p.product_name,
        y.YearlyProductSale,
        SUM(y.YearlyProductSale) OVER (PARTITION BY p.product_name) AS TotalProductSale
    FROM
        yearly_summary_of_products y
    LEFT JOIN
        gold.dim_products p ON y.product_key = p.product_key
)
SELECT
    *,
    DENSE_RANK() OVER (ORDER BY TotalProductSale DESC) AS RankingByTotalSales
FROM
    total_sales_for_each_product;
GO

-- =============================================================================
-- Product Category Analysis by Year
-- =============================================================================
CREATE VIEW gold.product_category_analysis_by_year AS
WITH yearly_summary_of_category AS (
    SELECT
        YEAR(order_date) AS order_year,
        product_key,
        SUM(sales_amount) AS YearlyProductSale
    FROM
        gold.fact_sales
    WHERE
        YEAR(order_date) IS NOT NULL
    GROUP BY
        YEAR(order_date),
        product_key
),
total_sales_for_each_category AS (
    SELECT
        y.order_year,
        p.category,
        SUM(y.YearlyProductSale) AS YearlyCategorySale
    FROM
        yearly_summary_of_category y
    LEFT JOIN
        gold.dim_products p ON y.product_key = p.product_key
    WHERE
        p.category IS NOT NULL
    GROUP BY
        y.order_year,
        p.category
)
SELECT
    *,
    SUM(YearlyCategorySale) OVER (PARTITION BY category) AS TotalCategorySale,
    DENSE_RANK() OVER (ORDER BY SUM(YearlyCategorySale) OVER (PARTITION BY category) DESC) AS RankingByTotalSales
FROM
    total_sales_for_each_category;
GO

-- =============================================================================
-- Age Group Spending Analysis
-- =============================================================================
CREATE VIEW gold.age_group_spending AS
SELECT
    rc.age_group,
    SUM(fs.sales_amount) AS total_sales,
    COUNT(*) AS total_orders,
    AVG(fs.sales_amount) AS average_order_value
FROM
    gold.report_customers AS rc
RIGHT JOIN
    gold.fact_sales fs ON rc.customer_key = fs.customer_key
WHERE
    rc.age_group IS NOT NULL
GROUP BY
    rc.age_group;
GO

-- =============================================================================
-- Age Group Product Report
-- =============================================================================
CREATE VIEW gold.age_group_report AS
WITH product_name_group AS (
    -- First CTE: Calculates product_quantity for each product within age_group, category, subcategory
    -- and ranks products by quantity within their age group.
    SELECT
        rc.age_group,
        rp.category,
        rp.subcategory,
        rp.product_name,
        SUM(fs.quantity) AS product_quantity,
        DENSE_RANK() OVER (PARTITION BY rc.age_group ORDER BY SUM(fs.quantity) DESC) AS Ranking_by_product_quantity
    FROM
        gold.report_customers AS rc
    LEFT JOIN
        gold.fact_sales fs ON rc.customer_key = fs.customer_key
    LEFT JOIN
        gold.report_products AS rp ON rp.product_key = fs.product_key
    GROUP BY
        rc.age_group,
        rp.category,
        rp.subcategory,
        rp.product_name
),
aggregated_quantities AS (
    -- Second CTE: Calculates the total quantity for each category and subcategory
    -- within each age group using window functions on the pre-calculated product_quantity.
    SELECT
        age_group,
        category,
        subcategory,
        product_name,
        product_quantity,
        Ranking_by_product_quantity,
        SUM(product_quantity) OVER (PARTITION BY age_group, category) AS category_quantity,
        SUM(product_quantity) OVER (PARTITION BY age_group, subcategory) AS subcategory_quantity
    FROM
        product_name_group
)
SELECT
    age_group,
    category,
    subcategory,
    product_name,
    product_quantity,
    Ranking_by_product_quantity,
    category_quantity,
    subcategory_quantity,
    DENSE_RANK() OVER (PARTITION BY age_group ORDER BY category_quantity DESC) AS Ranking_by_category_quantity,
    DENSE_RANK() OVER (PARTITION BY age_group ORDER BY subcategory_quantity DESC) AS Ranking_by_subcategory_quantity
FROM
    aggregated_quantities;
GO




