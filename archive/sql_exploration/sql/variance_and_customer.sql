-- sql/variance_and_customer.sql
-- Purpose:
-- Align SQL exploratory queries with the final Python pipeline logic.
-- This file contains separate queries for:
-- 1) monthly KPI with MoM
-- 2) next-month retention
-- 3) customer concentration
-- 4) cohort count output

------------------------------------------------------------
-- 1) Monthly KPI with MoM
-- Requires a monthly_kpi table produced from sql/kpi_metrics.sql
------------------------------------------------------------
WITH ordered_kpi AS (
    SELECT *
    FROM monthly_kpi
    ORDER BY month
)

SELECT
    month,
    net_gmv,
    product_sales_before_returns,
    "order",
    customer,
    unit_sold,
    avg_unit_price,
    return_value,
    return_unit,
    return_rate,
    top20_coverage,
    top50_coverage,
    hhi,
    active_order_days,
    calendar_days_in_month,
    is_partial_month,
    net_gmv_per_active_day,
    LAG(net_gmv) OVER (ORDER BY month) AS prev_net_gmv,
    CASE
        WHEN LAG(net_gmv) OVER (ORDER BY month) IS NULL THEN NULL
        ELSE (net_gmv - LAG(net_gmv) OVER (ORDER BY month)) * 1.0
             / NULLIF(LAG(net_gmv) OVER (ORDER BY month), 0)
    END AS net_gmv_mom,
    LAG(return_rate) OVER (ORDER BY month) AS prev_return_rate,
    CASE
        WHEN LAG(return_rate) OVER (ORDER BY month) IS NULL THEN NULL
        ELSE (return_rate - LAG(return_rate) OVER (ORDER BY month)) * 1.0
             / NULLIF(LAG(return_rate) OVER (ORDER BY month), 0)
    END AS return_rate_mom,
    LAG(top20_coverage) OVER (ORDER BY month) AS prev_top20_coverage,
    CASE
        WHEN LAG(top20_coverage) OVER (ORDER BY month) IS NULL THEN NULL
        ELSE (top20_coverage - LAG(top20_coverage) OVER (ORDER BY month)) * 1.0
             / NULLIF(LAG(top20_coverage) OVER (ORDER BY month), 0)
    END AS top20_coverage_mom,
    LAG(top50_coverage) OVER (ORDER BY month) AS prev_top50_coverage,
    CASE
        WHEN LAG(top50_coverage) OVER (ORDER BY month) IS NULL THEN NULL
        ELSE (top50_coverage - LAG(top50_coverage) OVER (ORDER BY month)) * 1.0
             / NULLIF(LAG(top50_coverage) OVER (ORDER BY month), 0)
    END AS top50_coverage_mom,
    LAG(hhi) OVER (ORDER BY month) AS prev_hhi,
    CASE
        WHEN LAG(hhi) OVER (ORDER BY month) IS NULL THEN NULL
        ELSE (hhi - LAG(hhi) OVER (ORDER BY month)) * 1.0
             / NULLIF(LAG(hhi) OVER (ORDER BY month), 0)
    END AS hhi_mom
FROM ordered_kpi
ORDER BY month;

------------------------------------------------------------
-- 2) Next-month retention (aligned with Python retention())
------------------------------------------------------------
WITH cust_month AS (
    SELECT DISTINCT
        month,
        CustomerID
    FROM metrics
    WHERE type = 'PRODUCT'
      AND CustomerID IS NOT NULL
),

month_pairs AS (
    SELECT
        month,
        LEAD(month) OVER (ORDER BY month) AS next_month
    FROM (
        SELECT DISTINCT month
        FROM cust_month
        ORDER BY month
    )
),

current_base AS (
    SELECT
        mp.month,
        COUNT(DISTINCT cm.CustomerID) AS current_customers
    FROM month_pairs mp
    LEFT JOIN cust_month cm
      ON mp.month = cm.month
    WHERE mp.next_month IS NOT NULL
    GROUP BY mp.month
),

repeat_counts AS (
    SELECT
        mp.month,
        COUNT(DISTINCT c.CustomerID) AS repeat_customers
    FROM month_pairs mp
    JOIN cust_month c
      ON mp.month = c.month
    JOIN cust_month n
      ON mp.next_month = n.month
     AND c.CustomerID = n.CustomerID
    WHERE mp.next_month IS NOT NULL
    GROUP BY mp.month
)

SELECT
    b.month,
    b.current_customers,
    COALESCE(r.repeat_customers, 0) AS repeat_customers,
    CASE
        WHEN b.current_customers > 0
            THEN COALESCE(r.repeat_customers, 0) * 1.0 / b.current_customers
        ELSE 0
    END AS next_month_repeat_rate
FROM current_base b
LEFT JOIN repeat_counts r
  ON b.month = r.month
ORDER BY b.month;

------------------------------------------------------------
-- 3) Customer concentration (aligned with Python customer_concentration(n=10))
------------------------------------------------------------
WITH cust_rev AS (
    SELECT
        month,
        CustomerID,
        SUM(net_gmv) AS cust_gmv
    FROM metrics
    WHERE type = 'PRODUCT'
      AND CustomerID IS NOT NULL
    GROUP BY month, CustomerID
),

cust_total AS (
    SELECT
        month,
        SUM(cust_gmv) AS cust_total_gmv
    FROM cust_rev
    GROUP BY month
),

cust_rank AS (
    SELECT
        month,
        CustomerID,
        cust_gmv,
        ROW_NUMBER() OVER (PARTITION BY month ORDER BY cust_gmv DESC) AS cust_rank
    FROM cust_rev
),

top10_cust AS (
    SELECT
        month,
        SUM(CASE WHEN cust_rank <= 10 THEN cust_gmv ELSE 0 END) AS top10_gmv
    FROM cust_rank
    GROUP BY month
),

cust_hhi AS (
    SELECT
        cr.month,
        SUM((cr.cust_gmv * 1.0 / ct.cust_total_gmv) * (cr.cust_gmv * 1.0 / ct.cust_total_gmv)) AS customer_hhi
    FROM cust_rev cr
    JOIN cust_total ct
      ON cr.month = ct.month
    GROUP BY cr.month
)

SELECT
    ct.month,
    CASE
        WHEN ct.cust_total_gmv > 0
            THEN COALESCE(t.top10_gmv, 0) * 1.0 / ct.cust_total_gmv
        ELSE 0
    END AS top10_share,
    COALESCE(h.customer_hhi, 0) AS customer_hhi
FROM cust_total ct
LEFT JOIN top10_cust t
  ON ct.month = t.month
LEFT JOIN cust_hhi h
  ON ct.month = h.month
ORDER BY ct.month;

------------------------------------------------------------
-- 4) Cohort count output (aligned with Python cohort_table())
------------------------------------------------------------
WITH customer_orders AS (
    SELECT DISTINCT
        month AS order_month,
        CustomerID
    FROM metrics
    WHERE type = 'PRODUCT'
      AND CustomerID IS NOT NULL
),

first_purchase AS (
    SELECT
        CustomerID,
        MIN(order_month) AS cohort_month
    FROM customer_orders
    GROUP BY CustomerID
),

cohort_base AS (
    SELECT
        co.order_month,
        fp.cohort_month,
        co.CustomerID,
        (
            (CAST(substr(co.order_month, 1, 4) AS INTEGER) - CAST(substr(fp.cohort_month, 1, 4) AS INTEGER)) * 12
            + (CAST(substr(co.order_month, 6, 2) AS INTEGER) - CAST(substr(fp.cohort_month, 6, 2) AS INTEGER))
        ) AS cohort_index
    FROM customer_orders co
    JOIN first_purchase fp
      ON co.CustomerID = fp.CustomerID
)

SELECT
    cohort_month,
    cohort_index,
    COUNT(DISTINCT CustomerID) AS customer_count
FROM cohort_base
GROUP BY cohort_month, cohort_index
ORDER BY cohort_month, cohort_index;