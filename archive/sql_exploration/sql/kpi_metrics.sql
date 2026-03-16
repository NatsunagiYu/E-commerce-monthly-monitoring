WITH dataset_bounds AS (
    SELECT
        MAX(order_date) AS dataset_end,
        strftime('%Y-%m', MAX(order_date)) AS last_month
    FROM metrics
),

product_lines AS (
    SELECT *
    FROM metrics
    WHERE type = 'PRODUCT'
),

monthly_core AS (
    SELECT
        month,
        SUM(net_gmv) AS net_gmv,
        COUNT(DISTINCT InvoiceNo) AS "order",
        COUNT(DISTINCT CustomerID) AS customer,
        SUM(Quantity) AS unit_sold,
        AVG(UnitPrice) AS avg_unit_price,
        COUNT(DISTINCT order_date) AS active_order_days
    FROM product_lines
    GROUP BY month
),

returns_monthly AS (
    SELECT
        month,
        SUM(return_value) AS return_value,
        SUM(abs_quantity) AS return_unit
    FROM metrics
    WHERE is_return = 1
    GROUP BY month
),

sku_monthly AS (
    SELECT
        month,
        StockCode,
        SUM(net_gmv) AS sku_gmv
    FROM product_lines
    GROUP BY month, StockCode
),

sku_monthly_rank AS (
    SELECT
        month,
        StockCode,
        sku_gmv,
        ROW_NUMBER() OVER (PARTITION BY month ORDER BY sku_gmv DESC) AS sku_rank
    FROM sku_monthly
),

topn_summary AS (
    SELECT
        month,
        SUM(CASE WHEN sku_rank <= 20 THEN sku_gmv ELSE 0 END) AS top20_gmv,
        SUM(CASE WHEN sku_rank <= 50 THEN sku_gmv ELSE 0 END) AS top50_gmv
    FROM sku_monthly_rank
    GROUP BY month
),

month_total AS (
    SELECT
        month,
        SUM(sku_gmv) AS total_gmv
    FROM sku_monthly
    GROUP BY month
),

hhi_calc AS (
    SELECT
        s.month,
        SUM((s.sku_gmv * 1.0 / t.total_gmv) * (s.sku_gmv * 1.0 / t.total_gmv)) AS hhi
    FROM sku_monthly s
    JOIN month_total t
      ON s.month = t.month
    GROUP BY s.month
),

calendar_info AS (
    SELECT DISTINCT
        month,
        CAST(
            strftime(
                '%d',
                date(month || '-01', 'start of month', '+1 month', '-1 day')
            ) AS INTEGER
        ) AS calendar_days_in_month
    FROM metrics
)

SELECT
    c.month,
    c.net_gmv,
    c."order",
    c.customer,
    c.unit_sold,
    c.avg_unit_price,
    COALESCE(r.return_value, 0) AS return_value,
    COALESCE(r.return_unit, 0) AS return_unit,
    CASE
        WHEN (c.net_gmv + ABS(COALESCE(r.return_value, 0))) > 0
            THEN ABS(COALESCE(r.return_value, 0)) * 1.0
                 / (c.net_gmv + ABS(COALESCE(r.return_value, 0)))
        ELSE 0
    END AS return_rate,
    CASE
        WHEN mt.total_gmv > 0
            THEN COALESCE(t.top20_gmv, 0) * 1.0 / mt.total_gmv
        ELSE 0
    END AS top20_coverage,
    CASE
        WHEN mt.total_gmv > 0
            THEN COALESCE(t.top50_gmv, 0) * 1.0 / mt.total_gmv
        ELSE 0
    END AS top50_coverage,
    COALESCE(h.hhi, 0) AS hhi,
    c.active_order_days,
    cal.calendar_days_in_month,
    CASE
        WHEN c.month = db.last_month
         AND date(db.dataset_end) < date(c.month || '-01', 'start of month', '+1 month', '-1 day')
            THEN 1
        ELSE 0
    END AS is_partial_month,
    CASE
        WHEN c.active_order_days > 0
            THEN c.net_gmv * 1.0 / c.active_order_days
        ELSE 0
    END AS net_gmv_per_active_day,
    c.net_gmv AS product_sales_before_returns
FROM monthly_core c
LEFT JOIN returns_monthly r
  ON c.month = r.month
LEFT JOIN topn_summary t
  ON c.month = t.month
LEFT JOIN month_total mt
  ON c.month = mt.month
LEFT JOIN hhi_calc h
  ON c.month = h.month
LEFT JOIN calendar_info cal
  ON c.month = cal.month
CROSS JOIN dataset_bounds db
ORDER BY c.month;