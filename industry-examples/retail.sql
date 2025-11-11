-- =========================================
-- RETAIL E-COMMERCE PIPELINE (PHYSICAL CTAS)
-- Namespace: dremio.retail
-- =========================================

-- -------------------------------
-- Namespaces
-- -------------------------------
CREATE FOLDER IF NOT EXISTS dremio.retail;
CREATE FOLDER IF NOT EXISTS dremio.retail.raw;
CREATE FOLDER IF NOT EXISTS dremio.retail.silver;
CREATE FOLDER IF NOT EXISTS dremio.retail.gold;

-- -------------------------------
-- RAW: Customers
-- -------------------------------
CREATE TABLE IF NOT EXISTS dremio.retail.raw.customers (
  customer_id   VARCHAR,
  email         VARCHAR,
  signup_ts     TIMESTAMP,
  country       VARCHAR,
  state         VARCHAR,
  loyalty_tier  VARCHAR          -- NULL, BRONZE, SILVER, GOLD, PLATINUM
);

INSERT INTO dremio.retail.raw.customers
(customer_id, email, signup_ts, country, state, loyalty_tier) VALUES
  ('C001','alice@example.com', TIMESTAMP '2025-07-01 10:05:00','US','NY','SILVER'),
  ('C002','bob@example.com',   TIMESTAMP '2025-07-15 14:22:00','US','CA',NULL),
  ('C003','cara@example.com',  TIMESTAMP '2025-08-02 09:10:00','US','TX','GOLD'),
  ('C004','dave@example.com',  TIMESTAMP '2025-08-20 17:44:00','US','FL','BRONZE');

-- -------------------------------
-- RAW: Orders
-- -------------------------------
CREATE TABLE IF NOT EXISTS dremio.retail.raw.orders (
  order_id        BIGINT,
  customer_id     VARCHAR,
  order_ts        TIMESTAMP,
  status          VARCHAR,        -- created, paid, shipped, canceled (free text from source)
  payment_method  VARCHAR,        -- CARD / PAYPAL / CASHAPP
  promo_code      VARCHAR
)
PARTITION BY (DAY(order_ts));

INSERT INTO dremio.retail.raw.orders
(order_id, customer_id, order_ts, status, payment_method, promo_code) VALUES
  (5001,'C001',TIMESTAMP '2025-08-10 11:00:00','Paid','CARD','SUMMER10'),
  (5002,'C002',TIMESTAMP '2025-08-11 16:45:00','created','PAYPAL',NULL),
  (5003,'C001',TIMESTAMP '2025-08-12 08:30:00','PAID','CARD',NULL),
  (5004,'C003',TIMESTAMP '2025-08-18 13:05:00','Shipped','CARD','VIP20'),
  (5005,'C004',TIMESTAMP '2025-08-19 09:15:00','canceled','CASHAPP',NULL);

-- -------------------------------
-- RAW: Order Items
-- -------------------------------
CREATE TABLE IF NOT EXISTS dremio.retail.raw.order_items (
  order_id     BIGINT,
  line_num     INT,
  sku          VARCHAR,
  category     VARCHAR,           -- 'Apparel', 'Electronics', 'Home'
  qty          INT,
  unit_price   DECIMAL(10,2),
  item_ts      TIMESTAMP          -- use order timestamp from source system
)
PARTITION BY (DAY(item_ts));

INSERT INTO dremio.retail.raw.order_items
(order_id, line_num, sku, category, qty, unit_price, item_ts) VALUES
  (5001,1,'SKU-TSHIRT','Apparel',     2, 19.99, TIMESTAMP '2025-08-10 11:00:05'),
  (5001,2,'SKU-HAT',   'Apparel',     1, 14.99, TIMESTAMP '2025-08-10 11:00:06'),
  (5002,1,'SKU-MUG',   'Home',        3,  9.50, TIMESTAMP '2025-08-11 16:45:10'),
  (5003,1,'SKU-EARBUD','Electronics', 1, 79.00, TIMESTAMP '2025-08-12 08:30:07'),
  (5003,2,'SKU-CASE',  'Electronics', 1, 15.00, TIMESTAMP '2025-08-12 08:30:08'),
  (5004,1,'SKU-JACKET','Apparel',     1,129.00, TIMESTAMP '2025-08-18 13:05:20'),
  (5005,1,'SKU-LAMP',  'Home',        2, 22.00, TIMESTAMP '2025-08-19 09:15:12');

-- ============================================================
-- SILVER LAYER (Physical CTAS)
-- Each step DROPs then recreates a physical table via CTAS
-- ============================================================

-- ---------------------------------
-- SILVER Step 1: Clean & standardize orders
--   - normalize status to upper
--   - derive order_date
-- ---------------------------------
DROP TABLE IF EXISTS dremio.retail.silver.orders_clean;
CREATE TABLE dremio.retail.silver.orders_clean
PARTITION BY (order_date)
AS
SELECT
  o.order_id,
  o.customer_id,
  o.order_ts,
  UPPER(TRIM(o.status))                      AS status_std,
  o.payment_method,
  o.promo_code,
  TO_DATE(o.order_ts)                        AS order_date
FROM dremio.retail.raw.orders o;

-- ---------------------------------
-- SILVER Step 2: Deduplicate items (defensive),
-- compute line_total, flag promo usage
-- ---------------------------------
DROP TABLE IF EXISTS dremio.retail.silver.order_lines;
CREATE TABLE dremio.retail.silver.order_lines
PARTITION BY (order_date)
AS
SELECT
  i.order_id,
  i.line_num,
  i.sku,
  i.category,
  i.qty,
  i.unit_price,
  CAST(i.qty * i.unit_price AS DECIMAL(18,2)) AS line_total,
  TO_DATE(i.item_ts)                            AS order_date
FROM (
  SELECT
    order_id, line_num, sku, category, qty, unit_price, item_ts,
    ROW_NUMBER() OVER (PARTITION BY order_id, line_num ORDER BY item_ts DESC) AS rn
  FROM dremio.retail.raw.order_items
) i
WHERE i.rn = 1;

-- ---------------------------------
-- SILVER Step 3: Order facts (join orders + items),
-- derive order_status_flags and totals
-- ---------------------------------
DROP TABLE IF EXISTS dremio.retail.silver.order_facts;
CREATE TABLE dremio.retail.silver.order_facts
PARTITION BY (order_date)
DISTRIBUTE BY (order_id)
AS
SELECT
  o.order_id,
  o.customer_id,
  o.order_ts,
  o.order_date,
  o.status_std,
  o.payment_method,
  o.promo_code,
  CASE WHEN o.promo_code IS NOT NULL THEN 1 ELSE 0 END          AS used_promo,
  l.sku,
  l.category,
  l.qty,
  l.unit_price,
  l.line_total
FROM dremio.retail.silver.orders_clean o
JOIN dremio.retail.silver.order_lines l
  ON o.order_id = l.order_id;

-- ---------------------------------
-- SILVER Step 4: Customer-enriched order facts
-- ---------------------------------
DROP TABLE IF EXISTS dremio.retail.silver.customer_order_facts;
CREATE TABLE dremio.retail.silver.customer_order_facts
PARTITION BY (order_date)
AS
SELECT
  f.*,
  c.email,
  c.signup_ts,
  c.country,
  c.state,
  COALESCE(c.loyalty_tier,'UNASSIGNED') AS loyalty_tier
FROM dremio.retail.silver.order_facts f
LEFT JOIN dremio.retail.raw.customers c
  ON f.customer_id = c.customer_id;

-- ---------------------------------
-- SILVER Step 5: Order-level totals (collapse lines to orders)
-- ---------------------------------
DROP TABLE IF EXISTS dremio.retail.silver.order_totals;
CREATE TABLE dremio.retail.silver.order_totals
PARTITION BY (order_date)
AS
SELECT
  order_id,
  customer_id,
  order_date,
  status_std,
  payment_method,
  MAX(used_promo)                                       AS used_promo,
  COUNT(*)                                              AS line_count,
  CAST(SUM(line_total) AS DECIMAL(18,2))                AS order_total
FROM dremio.retail.silver.order_facts
GROUP BY order_id, customer_id, order_date, status_std, payment_method;

-- ============================================================
-- GOLD LAYER (Physical CTAS)
-- Business-ready marts / KPIs
-- ============================================================

-- ---------------------------------
-- GOLD 1: Daily sales summary
-- ---------------------------------
DROP TABLE IF EXISTS dremio.retail.gold.daily_sales;
CREATE TABLE dremio.retail.gold.daily_sales
PARTITION BY (order_date)
AS
SELECT
  t.order_date,
  COUNT(DISTINCT t.order_id)                                 AS orders,
  COUNT(DISTINCT t.customer_id)                              AS distinct_customers,
  CAST(SUM(CASE WHEN t.status_std <> 'CANCELED' THEN t.order_total ELSE 0 END)
      AS DECIMAL(18,2))                                      AS gross_revenue,
  CAST(SUM(CASE WHEN t.used_promo = 1 AND t.status_std <> 'CANCELED' THEN t.order_total ELSE 0 END)
      AS DECIMAL(18,2))                                      AS promo_revenue
FROM dremio.retail.silver.order_totals t
GROUP BY t.order_date;

-- ---------------------------------
-- GOLD 2: Category daily revenue with 7-day rolling window
-- ---------------------------------
DROP TABLE IF EXISTS dremio.retail.gold.category_rolling7;
CREATE TABLE dremio.retail.gold.category_rolling7
PARTITION BY (category, order_date)
AS
WITH per_day AS (
  SELECT
    f.order_date,
    f.category,
    CAST(SUM(CASE WHEN f.status_std <> 'CANCELED' THEN f.line_total ELSE 0 END)
        AS DECIMAL(18,2)) AS day_revenue
  FROM dremio.retail.silver.order_facts f
  GROUP BY f.order_date, f.category
)
SELECT
  category,
  order_date,
  day_revenue,
  -- 7-day rolling sum per category (includes current day)
  CAST(SUM(day_revenue) OVER (
        PARTITION BY category
        ORDER BY order_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
      ) AS DECIMAL(18,2)) AS revenue_rolling_7d
FROM per_day;

-- ---------------------------------
-- GOLD 3: Loyalty cohort LTV (first 90 days from first order)
-- ---------------------------------
DROP TABLE IF EXISTS dremio.retail.gold.cohort_ltv_90d;
CREATE TABLE dremio.retail.gold.cohort_ltv_90d
AS
WITH customer_first AS (
  SELECT
    t.customer_id,
    MIN(t.order_date) AS first_order_date
  FROM dremio.retail.silver.order_totals t
  WHERE t.status_std <> 'CANCELED'
  GROUP BY t.customer_id
),
windowed AS (
  SELECT
    t.customer_id,
    t.order_date,
    t.order_total,
    cf.first_order_date,
    CASE
      WHEN t.order_date BETWEEN cf.first_order_date
                            AND (cf.first_order_date + INTERVAL '90' DAY)
      THEN 1 ELSE 0
    END AS in_90d
  FROM dremio.retail.silver.order_totals t
  JOIN customer_first cf ON t.customer_id = cf.customer_id
  WHERE t.status_std <> 'CANCELED'
)
SELECT
  w.customer_id,
  w.first_order_date,
  CAST(SUM(CASE WHEN w.in_90d = 1 THEN w.order_total ELSE 0 END) AS DECIMAL(18,2)) AS ltv_90d
FROM windowed w
GROUP BY w.customer_id, w.first_order_date;

-- ---------------------------------
-- GOLD 4: Payment mix by day
-- ---------------------------------
DROP TABLE IF EXISTS dremio.retail.gold.payment_mix_by_day;
CREATE TABLE dremio.retail.gold.payment_mix_by_day
PARTITION BY (order_date)
AS
SELECT
  order_date,
  payment_method,
  COUNT(*)                                                   AS orders,
  CAST(SUM(CASE WHEN status_std <> 'CANCELED' THEN order_total ELSE 0 END)
      AS DECIMAL(18,2))                                     AS revenue
FROM dremio.retail.silver.order_totals
GROUP BY order_date, payment_method;

-- ---------------------------------
-- GOLD 5: State-level daily revenue
-- ---------------------------------
DROP TABLE IF EXISTS dremio.retail.gold.state_daily_revenue;
CREATE TABLE dremio.retail.gold.state_daily_revenue
PARTITION BY (state, order_date)
AS
SELECT
  cof.state,
  t.order_date,
  CAST(SUM(CASE WHEN t.status_std <> 'CANCELED' THEN t.order_total ELSE 0 END)
      AS DECIMAL(18,2)) AS revenue
FROM dremio.retail.silver.order_totals t
JOIN dremio.retail.silver.customer_order_facts cof
  ON t.order_id = cof.order_id
GROUP BY cof.state, t.order_date;

-- =========================================
-- Done. Example physical pipeline created.
--   dremio.retail.raw.*
--   dremio.retail.silver.*
--   dremio.retail.gold.*
-- =========================================
