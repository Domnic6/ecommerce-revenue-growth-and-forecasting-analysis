--Ecommerce Revenue Growth & Forecasting Analysis
--Dataset: thelook_ecommerce
--Platform: Google BigQuery

-- Section 1 __BASE FACT TABLES__

--1. FACT TABLE : ALL ORDER ITEMS
--Purpose : Cleaned transaction - level base table used for downstream analysis

CREATE OR REPLACE TABLE analytics.fact_order_items_all AS
SELECT
 oi.user_id,
 oi.product_id,
 oi.order_id,
 DATE(oi.created_at) AS order_date,
 oi.sale_price AS revenue,
 oi.status
FROM
`bigquery-public-data.thelook_ecommerce.order_items` oi;

--2. FACT TABLE : COMPLETED/ SUCCESSFUL ORDER ITEMS
--Purpose : Revenue - related analysis using valid purchase statuses
--NOTE: This table includes commercially valid revenue statuses
--(Processing, Shipped, Complete), not only fully completed orders

CREATE OR REPLACE TABLE analytics.fact_order_items_completed AS
SELECT *
FROM 
analytics.fact_order_items_all
WHERE status IN ('Processing', 'Shipped', 'Complete');

--MONTHLY GROWTH KPI TABLE
--Purpose : Monthly revenue, orders, users, revenue per user, cancellation metrics, MoM growth

--6.Aggregate monthly KPI's

CREATE OR REPLACE TABLE analytics.agg_monthly_kpis AS
WITH monthly_orders AS(
  SELECT
   DATE_TRUNC(order_date, MONTH) AS month,
   COUNT(DISTINCT order_id) AS total_orders_all,
   COUNT(DISTINCT IF(status = 'Cancelled', order_id, NULL)) AS cancelled_orders
  FROM analytics.fact_order_items_all
  GROUP BY month
),

monthly_revenue AS(
  SELECT
   DATE_TRUNC(order_date, MONTH) AS month,
   SUM(revenue) AS total_revenue,
   COUNT(DISTINCT order_id) AS completed_orders,
   COUNT(DISTINCT user_id) AS purchasing_users
  FROM analytics.fact_order_items_completed
  GROUP BY month 
)

SELECT
 r.month,
 r.total_revenue,
 r.completed_orders AS total_orders,
 r.purchasing_users,
 SAFE_DIVIDE(r.total_revenue, r.completed_orders) AS avg_order_value,
 SAFE_DIVIDE(r.total_revenue, r.purchasing_users) AS revenue_per_user,
 o.total_orders_all,
 o.cancelled_orders,
 SAFE_DIVIDE(o.cancelled_orders, o.total_orders_all) AS cancellation_rate
FROM monthly_revenue r
LEFT JOIN monthly_orders o
 ON r.month = o.month
ORDER  BY r.month;

--7. AGGREGATE MONTHLY KPIS ENRICHED
--Purpose : Monthly revenue and order growth 
CREATE OR REPLACE TABLE analytics.agg_monthly_kpis_enriched AS
SELECT
  month,
  total_revenue,
  total_orders,
  purchasing_users,
  revenue_per_user,
  cancellation_rate,
  cancelled_revenue,

  -- MoM % change
  SAFE_DIVIDE(
    total_revenue - LAG(total_revenue) OVER (ORDER BY month),
    LAG(total_revenue) OVER (ORDER BY month)
  )AS revenue_mom_pct,

  SAFE_DIVIDE(
    total_orders - LAG(total_orders) OVER (ORDER BY month),
    LAG(total_orders) OVER (ORDER BY month)
  )AS orders_mom_pct

FROM analytics.agg_monthly_kpis
ORDER BY month;

--8.YEARLY GROWTH KPI TABLE
--Purpose : Yearly revenue, orders, YoY growth

CREATE OR REPLACE TABLE analytics.agg_yearly_kpis AS 
SELECT
 EXTRACT(YEAR FROM month) AS year,
 SUM(total_revenue) AS total_revenue,
 SUM(total_orders) AS total_orders
FROM analytics.agg_monthly_kpis
GROUP BY year
ORDER BY year;

--9. YEARLY GROWTH KPI TABLE (ENRICHED)
--Purpose : Adds YoY revenue and order growth to yearly KPI base table
CREATE OR REPLACE TABLE analytics.agg_yearly_kpis_enriched AS 

SELECT
 year,
 total_revenue,
 total_orders,

 SAFE_DIVIDE(
  total_revenue-LAG(total_revenue) OVER (ORDER BY year),
  LAG(total_revenue) OVER (ORDER BY year)
 ) AS revenue_yoy_pct,

 SAFE_DIVIDE(
  total_orders-LAG(total_orders) OVER (ORDER BY year),
  LAG(total_orders) OVER (ORDER BY year)
 )AS orders_yoy_pct
FROM analytics.agg_yearly_kpis
ORDER BY year;


