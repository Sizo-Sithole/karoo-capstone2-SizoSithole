USE DATABASE DB1;
USE SCHEMA PUBLIC;

CREATE OR REPLACE VIEW v_supplier_health AS
WITH certs AS (
  SELECT
    supplier_id,
    MIN(expiry_date) AS next_cert_expiry
  FROM CERTIFICATIONS
  GROUP BY supplier_id
),
orders_agg AS (
  SELECT
    supplier_id,
    SUM(CASE WHEN order_date >= DATEADD(day, -90, CURRENT_DATE()) THEN 1 ELSE 0 END) AS orders_90d
  FROM ORDERSS
  GROUP BY supplier_id
),
harvest_calc AS (
  SELECT
    supplier_id,
    harvest_date,
    quantity_kg AS yield_kg,
    AVG(quantity_kg) OVER (
      PARTITION BY supplier_id
      ORDER BY harvest_date
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_avg_yield,
    ROW_NUMBER() OVER (
      PARTITION BY supplier_id
      ORDER BY harvest_date DESC
    ) AS rn
  FROM HARVEST_LOG
),
latest_harvest AS (
  SELECT
    supplier_id,
    harvest_date AS latest_harvest_date,
    yield_kg AS latest_yield,
    rolling_avg_yield
  FROM harvest_calc
  WHERE rn = 1
)
SELECT
  s.supplier_id,
  s.farm_name,
  s.region,
  s.status,
  s.last_audit,
  CASE
    WHEN c.next_cert_expiry IS NULL THEN 'Unknown'
    WHEN c.next_cert_expiry < CURRENT_DATE() THEN 'Expired'
    WHEN c.next_cert_expiry <= DATEADD(day, 30, CURRENT_DATE()) THEN 'Expiring Soon'
    ELSE 'Valid'
  END AS cert_status,
  c.next_cert_expiry,
  COALESCE(o.orders_90d, 0) AS orders_90d,
  lh.latest_harvest_date,
  lh.latest_yield,
  lh.rolling_avg_yield
FROM SUPPLIERS s
LEFT JOIN certs c ON c.supplier_id = s.supplier_id
LEFT JOIN orders_agg o ON o.supplier_id = s.supplier_id
LEFT JOIN latest_harvest lh ON lh.supplier_id = s.supplier_id;

SELECT
  supplier_id,
  farm_name,
  region,
  cert_status,
  next_cert_expiry,
  orders_90d,
  latest_yield,
  rolling_avg_yield,
  CASE WHEN next_cert_expiry IS NOT NULL AND next_cert_expiry <= DATEADD(day, 30, CURRENT_DATE()) THEN 1 ELSE 0 END AS flag_cert_30d,
  CASE WHEN orders_90d = 0 THEN 1 ELSE 0 END AS flag_zero_orders_90d,
  CASE
    WHEN latest_yield IS NULL OR rolling_avg_yield IS NULL THEN 0
    WHEN latest_yield < (0.8 * rolling_avg_yield) THEN 1
    ELSE 0
  END AS flag_yield_decline
FROM v_supplier_health
WHERE
  (next_cert_expiry IS NOT NULL AND next_cert_expiry <= DATEADD(day, 30, CURRENT_DATE()))
  OR (orders_90d = 0)
  OR (latest_yield IS NOT NULL AND rolling_avg_yield IS NOT NULL AND latest_yield < (0.8 * rolling_avg_yield))
ORDER BY region, supplier_id;
