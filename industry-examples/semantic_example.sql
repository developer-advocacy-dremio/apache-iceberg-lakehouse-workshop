-- =========================================
-- Namespaces
-- =========================================
CREATE FOLDER IF NOT EXISTS dremio.subscriptions;
CREATE FOLDER IF NOT EXISTS dremio.subscriptions.raw;
CREATE FOLDER IF NOT EXISTS dremio.subscriptions.silver;
CREATE FOLDER IF NOT EXISTS dremio.subscriptions.gold;

-- =========================================
-- RAW: Customer master dump (opaque CRM extract)
-- - Messy column names
-- - Encoded fields (C/T for active flag)
-- - Mixed timestamp formats
-- - No enforced types
-- =========================================
CREATE TABLE IF NOT EXISTS dremio.subscriptions.raw.cust_mstr (
  cid         VARCHAR,        -- customer id
  c_nm        VARCHAR,        -- full name
  c_eml       VARCHAR,        -- email address
  act_flg     VARCHAR,        -- C = current, T = terminated
  crt_ts      VARCHAR         -- creation timestamp as string
);

INSERT INTO dremio.subscriptions.raw.cust_mstr VALUES
  ('C001','Alice North','alice@example.com','C','2025/08/18 09:15:33'),
  ('C002','Bob West','bob@example.com','C','2025-08-18T10:01:00Z'),
  ('C003','Cara South','cara@example.com','T','18-08-2025 11:45');


-- =========================================
-- RAW: Subscription plans (ERP export)
-- - Unclear column names
-- - Numeric codes for plan tiers
-- - Currency not normalized
-- =========================================
CREATE TABLE IF NOT EXISTS dremio.subscriptions.raw.plan_tbl (
  pid          VARCHAR,      -- plan id
  p_desc       VARCHAR,      -- plan description
  p_cd         INTEGER,      -- tier code (1,2,3)
  p_amt        DECIMAL(10,2),-- monthly amount
  curr         VARCHAR       -- currency code
);

INSERT INTO dremio.subscriptions.raw.plan_tbl VALUES
  ('P01','Starter',1,25.00,'usd'),
  ('P02','Growth',2,60.00,'USD'),
  ('P03','Enterprise',3,220.00,'Usd');


-- =========================================
-- RAW: Subscriptions linking customers to plans
-- - Mixed date formats
-- - Status codes unclear
-- - Renewal flag ambiguous
-- =========================================
CREATE TABLE IF NOT EXISTS dremio.subscriptions.raw.subscrpt (
  sid        VARCHAR,     -- subscription id
  c_id       VARCHAR,     -- customer id
  pl_id      VARCHAR,     -- plan id
  st_cd      VARCHAR,     -- A = active, X = canceled
  st_dt      VARCHAR,     -- start date (mixed formats)
  rn_flg     VARCHAR      -- Y/N renewal flag
);

INSERT INTO dremio.subscriptions.raw.subscrpt VALUES
  ('S001','C001','P01','A','2025-08-01','Y'),
  ('S002','C002','P03','A','01/08/2025','N'),
  ('S003','C003','P02','X','2025/08/05','Y');


-- =========================================
-- RAW: Product usage events (from app logs)
-- - Event codes instead of names
-- - Unclear fields
-- - Epoch + ISO timestamps mixed
-- =========================================
CREATE TABLE IF NOT EXISTS dremio.subscriptions.raw.usage_evt (
  evt_id     VARCHAR,
  cust       VARCHAR,         -- customer id
  pl         VARCHAR,         -- plan id
  u_ts       VARCHAR,         -- timestamp (epoch or ISO)
  u_cd       VARCHAR,         -- event code (LGIN, ACTN, ERR)
  qty_v      INTEGER          -- value associated with event
);

INSERT INTO dremio.subscriptions.raw.usage_evt VALUES
  ('E1001','C001','P01','2025-08-18T12:10:00Z','LGIN',1),
  ('E1002','C001','P01','1692369600','ACTN',4),
  ('E1003','C002','P03','2025/08/18 14:22','ACTN',2),
  ('E1004','C003','P02','1692373200','ERR',1);

-- =========================================
-- SILVER: Cleaned customer records
-- - Normalize active flag
-- - Standardize timestamps
-- - Rename opaque columns
-- =========================================
CREATE OR REPLACE VIEW dremio.subscriptions.silver.customers AS
SELECT
  cid                AS customer_id,
  c_nm               AS full_name,
  c_eml              AS email,

  CASE 
    WHEN act_flg = 'C' THEN 'ACTIVE'
    WHEN act_flg = 'T' THEN 'TERMINATED'
    ELSE 'UNKNOWN'
  END AS status,

  -- Convert mixed timestamp strings into proper TIMESTAMP
  TRY_CAST(crt_ts AS TIMESTAMP) AS created_at
FROM dremio.subscriptions.raw.cust_mstr;


-- =========================================
-- SILVER: Cleaned plan table
-- - Normalize currency
-- - Convert tier codes into meaningful values
-- - Rename columns to semantic names
-- =========================================
CREATE OR REPLACE VIEW dremio.subscriptions.silver.plans AS
SELECT
  pid                    AS plan_id,
  p_desc                 AS plan_name,

  CASE p_cd
    WHEN 1 THEN 'Starter'
    WHEN 2 THEN 'Growth'
    WHEN 3 THEN 'Enterprise'
    ELSE 'Unknown'
  END AS plan_tier,

  p_amt                  AS monthly_price,
  UPPER(curr)            AS currency
FROM dremio.subscriptions.raw.plan_tbl;


-- =========================================
-- SILVER: Cleaned subscriptions
-- - Normalize status codes
-- - Standardize start dates
-- - Rename columns to business-friendly names
-- =========================================
CREATE OR REPLACE VIEW dremio.subscriptions.silver.subscriptions AS
SELECT
  sid                          AS subscription_id,
  c_id                         AS customer_id,
  pl_id                        AS plan_id,

  CASE 
    WHEN st_cd = 'A' THEN 'ACTIVE'
    WHEN st_cd = 'X' THEN 'CANCELED'
    ELSE 'UNKNOWN'
  END AS subscription_status,

  TRY_CAST(st_dt AS DATE)      AS start_date,

  CASE rn_flg
    WHEN 'Y' THEN TRUE
    ELSE FALSE
  END AS auto_renew
FROM dremio.subscriptions.raw.subscrpt;


-- =========================================
-- SILVER: Cleaned usage events
-- - Parse mixed timestamps (epoch / ISO / slash)
-- - Convert event codes to descriptive names
-- - Rename fields to semantic names
-- =========================================
CREATE OR REPLACE VIEW dremio.subscriptions.silver.usage_events AS
SELECT
  evt_id                     AS event_id,
  cust                       AS customer_id,
  pl                         AS plan_id,

  -- Convert multiple timestamp formats
  CASE 
    WHEN u_ts LIKE '%-%' OR u_ts LIKE '%/%' THEN TRY_CAST(u_ts AS TIMESTAMP)
    ELSE TO_TIMESTAMP(CAST(u_ts AS BIGINT))     -- Epoch seconds
  END AS event_ts,

  CASE u_cd
    WHEN 'LGIN' THEN 'LOGIN'
    WHEN 'ACTN' THEN 'ACTION'
    WHEN 'ERR'  THEN 'ERROR'
    ELSE 'UNKNOWN'
  END AS event_type,

  qty_v                      AS event_value
FROM dremio.subscriptions.raw.usage_evt;

-- =========================================
-- GOLD: Active Subscriptions
-- - List all customers with currently active subscriptions
-- - Joins the Silver customer + subscription + plan models
-- =========================================
CREATE OR REPLACE VIEW dremio.subscriptions.gold.active_subscriptions AS
SELECT
  s.subscription_id,
  s.customer_id,
  c.full_name,
  c.email,
  p.plan_name,
  p.plan_tier,
  p.monthly_price,
  s.start_date,
  s.auto_renew
FROM dremio.subscriptions.silver.subscriptions s
JOIN dremio.subscriptions.silver.customers c
  ON s.customer_id = c.customer_id
JOIN dremio.subscriptions.silver.plans p
  ON s.plan_id = p.plan_id
WHERE s.subscription_status = 'ACTIVE'
  AND c.status = 'ACTIVE';

-- =========================================
-- GOLD: Monthly Recurring Revenue (MRR)
-- - Sums the price of all active subscriptions
-- - Uses the cleaned/typed Silver layer
-- =========================================
CREATE OR REPLACE VIEW dremio.subscriptions.gold.monthly_recurring_revenue AS
SELECT
  DATE_TRUNC('month', CURRENT_DATE) AS month_start,
  SUM(p.monthly_price) AS total_mrr
FROM dremio.subscriptions.silver.subscriptions s
JOIN dremio.subscriptions.silver.plans p
  ON s.plan_id = p.plan_id
WHERE s.subscription_status = 'ACTIVE';


-- =========================================
-- GOLD: Customer Usage Summary
-- - Aggregates usage events
-- - Counts logins, actions, errors per customer
-- =========================================
CREATE OR REPLACE VIEW dremio.subscriptions.gold.customer_usage_summary AS
SELECT
  e.customer_id,
  COUNT(CASE WHEN event_type = 'LOGIN'  THEN 1 END) AS login_count,
  COUNT(CASE WHEN event_type = 'ACTION' THEN 1 END) AS action_count,
  COUNT(CASE WHEN event_type = 'ERROR'  THEN 1 END) AS error_count,
  COUNT(*) AS total_events
FROM dremio.subscriptions.silver.usage_events e
GROUP BY e.customer_id;


-- =========================================
-- GOLD: Daily Active Users (DAU)
-- - Distinct customers generating any event per day
-- - Uses parsed timestamps from the Silver layer
-- =========================================
CREATE OR REPLACE VIEW dremio.subscriptions.gold.daily_active_users AS
SELECT
  DATE(event_ts) AS event_date,
  COUNT(DISTINCT customer_id) AS dau
FROM dremio.subscriptions.silver.usage_events
GROUP BY DATE(event_ts);


-- =========================================
-- GOLD: Churned Customers
-- - A customer is churned if they were TERMINATED or had a CANCELLED subscription
-- - Logic lives cleanly in the semantic layer
-- =========================================
CREATE OR REPLACE VIEW dremio.subscriptions.gold.churned_customers AS
SELECT DISTINCT
  c.customer_id,
  c.full_name,
  c.email,
  s.subscription_id,
  s.plan_id,
  s.start_date
FROM dremio.subscriptions.silver.customers c
LEFT JOIN dremio.subscriptions.silver.subscriptions s
  ON c.customer_id = s.customer_id
WHERE c.status = 'TERMINATED'
   OR s.subscription_status = 'CANCELED';
