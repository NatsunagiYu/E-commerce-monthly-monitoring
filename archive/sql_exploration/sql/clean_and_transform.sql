WITH base AS (
    SELECT
        InvoiceNo,
        StockCode,
        Description,
        Quantity,
        UnitPrice,
        CustomerID,
        Country,
        DATE(InvoiceDate) AS order_date,
        strftime('%Y-%m', InvoiceDate) AS month
    FROM online_retail
    WHERE InvoiceNo IS NOT NULL
      AND StockCode IS NOT NULL
      AND UnitPrice IS NOT NULL
      AND Quantity IS NOT NULL
      AND UnitPrice > 0
),

classified AS (
    SELECT
        *,
        CASE
            WHEN UPPER(TRIM(COALESCE(StockCode, ''))) = 'AMAZONFEE'
                 OR UPPER(COALESCE(Description, '')) LIKE '%AMAZON FEE%'
                THEN 'FEE'
            WHEN UPPER(TRIM(COALESCE(StockCode, ''))) = 'BANK CHARGES'
                 OR UPPER(COALESCE(Description, '')) LIKE '%BANK CHARGES%'
                THEN 'BANK_CHARGE'
            WHEN UPPER(TRIM(COALESCE(StockCode, ''))) = 'M'
                 OR UPPER(COALESCE(Description, '')) LIKE '%MANUAL%'
                THEN 'MANUAL'
            WHEN UPPER(TRIM(COALESCE(StockCode, ''))) = 'D'
                 OR UPPER(COALESCE(Description, '')) LIKE '%DISCOUNT%'
                THEN 'DISCOUNT'
            WHEN UPPER(COALESCE(Description, '')) LIKE '%POSTAGE%'
                THEN 'POSTAGE'
            WHEN UPPER(COALESCE(Description, '')) LIKE '%SAMPLES%'
                THEN 'SAMPLES'
            WHEN UPPER(TRIM(COALESCE(InvoiceNo, ''))) LIKE 'C%'
                 OR Quantity < 0
                THEN 'RETURN'
            ELSE 'PRODUCT'
        END AS type
    FROM base
),

metrics AS (
    SELECT
        *,
        CASE WHEN type = 'RETURN' THEN 1 ELSE 0 END AS is_return,
        CASE WHEN type = 'PRODUCT' THEN 1 ELSE 0 END AS is_product,
        CASE
            WHEN type IN ('FEE', 'BANK_CHARGE', 'MANUAL', 'DISCOUNT', 'POSTAGE', 'SAMPLES')
                THEN 1
            ELSE 0
        END AS is_non_merch_adjustment,
        UnitPrice * Quantity AS line_gmv,
        CASE
            WHEN type = 'PRODUCT' AND Quantity > 0
                THEN UnitPrice * Quantity
            ELSE 0
        END AS net_gmv,
        CASE
            WHEN type = 'RETURN'
                THEN UnitPrice * Quantity
            ELSE 0
        END AS return_value,
        ABS(Quantity) AS abs_quantity
    FROM classified
)

SELECT *
FROM metrics;