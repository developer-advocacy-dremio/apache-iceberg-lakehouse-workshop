-- =========================================
-- Namespaces
-- =========================================
CREATE FOLDER IF NOT EXISTS dremio.insurance;
CREATE FOLDER IF NOT EXISTS dremio.insurance.raw;
CREATE FOLDER IF NOT EXISTS dremio.insurance.silver;
CREATE FOLDER IF NOT EXISTS dremio.insurance.gold;

-- =========================================
-- BRONZE: Monthly policy premiums (earned)
-- One row per policy per month of exposure
-- =========================================
CREATE TABLE IF NOT EXISTS dremio.insurance.raw.policy_premiums (
  policy_id        VARCHAR,
  customer_id      VARCHAR,
  product_line     VARCHAR,          -- e.g., AUTO, HOME
  state            VARCHAR,          -- e.g., NY, CA, TX
  effective_date   DATE,
  expiry_date      DATE,
  premium_month    DATE,             -- first of month for exposure
  earned_premium   DECIMAL(18,2)
)
PARTITION BY (premium_month);

-- Sample data
INSERT INTO dremio.insurance.raw.policy_premiums
(policy_id, customer_id, product_line, state, effective_date, expiry_date, premium_month, earned_premium) VALUES
  ('POL-1001', 'C001', 'AUTO', 'NY', DATE '2025-07-15', DATE '2026-07-14', DATE '2025-08-01', 120.00),
  ('POL-1001', 'C001', 'AUTO', 'NY', DATE '2025-07-15', DATE '2026-07-14', DATE '2025-09-01', 120.00),
  ('POL-1002', 'C002', 'HOME', 'CA', DATE '2025-06-10', DATE '2026-06-09', DATE '2025-08-01',  80.00),
  ('POL-1003', 'C003', 'AUTO', 'NY', DATE '2025-08-01', DATE '2026-07-31', DATE '2025-08-01',  90.00),
  ('POL-1004', 'C004', 'AUTO', 'TX', DATE '2025-08-05', DATE '2026-08-04', DATE '2025-08-01', 100.00),
  ('POL-1004', 'C004', 'AUTO', 'TX', DATE '2025-08-05', DATE '2026-08-04', DATE '2025-09-01', 100.00);

-- =========================================
-- BRONZE: Claims
-- One row per claim (paid + reserve model)
-- =========================================
CREATE TABLE IF NOT EXISTS dremio.insurance.raw.claims (
  claim_id        BIGINT,
  policy_id       VARCHAR,
  accident_date   DATE,
  report_ts       TIMESTAMP,
  status          VARCHAR,           -- OPEN / CLOSED
  paid_amount     DECIMAL(18,2),
  reserve_amount  DECIMAL(18,2),
  settled_ts      TIMESTAMP          -- NULL if still open
)
PARTITION BY (DAY(report_ts));

-- Sample data
INSERT INTO dremio.insurance.raw.claims
(claim_id, policy_id, accident_date, report_ts, status, paid_amount, reserve_amount, settled_ts) VALUES
  (7001, 'POL-1001', DATE '2025-08-14', TIMESTAMP '2025-08-15 10:00:00', 'OPEN'  ,  500.00, 1000.00, NULL),
  (7002, 'POL-1002', DATE '2025-08-18', TIMESTAMP '2025-08-20 09:30:00', 'CLOSED', 2000.00,    0.00, TIMESTAMP '2025-08-25 16:00:00'),
  (7003, 'POL-1003', DATE '2025-09-03', TIMESTAMP '2025-09-05 08:45:00', 'OPEN'  ,  300.00,  700.00, NULL),
  (7004, 'POL-1001', DATE '2025-09-09', TIMESTAMP '2025-09-10 14:20:00', 'CLOSED',  800.00,    0.00, TIMESTAMP '2025-09-18 11:15:00');

-- =========================================
-- SILVER: Claims enriched with policy attributes & exposure month
-- Joins claims to the matching policy exposure month
-- =========================================
CREATE OR REPLACE VIEW dremio.insurance.silver.claims_enriched AS
SELECT
  c.claim_id,
  c.policy_id,
  p.product_line,
  p.state,
  p.premium_month,
  -- normalize claim to month bucket for aggregation
  TO_DATE(DATE_TRUNC('MONTH', c.report_ts))                     AS claim_month,
  c.accident_date,
  c.report_ts,
  c.status,
  c.paid_amount,
  c.reserve_amount,
  (c.paid_amount + c.reserve_amount)                            AS incurred_amount,
  p.earned_premium,
  p.effective_date,
  p.expiry_date,
  -- claim cycle time (0 for open claims)
  DATEDIFF(COALESCE(c.settled_ts, c.report_ts), c.report_ts)    AS claim_cycle_days
FROM dremio.insurance.raw.claims c
JOIN dremio.insurance.raw.policy_premiums p
  ON c.policy_id = p.policy_id
 AND TO_DATE(DATE_TRUNC('MONTH', c.report_ts)) = p.premium_month;

-- =========================================
-- GOLD: Monthly loss metrics by product/state
-- Loss Ratio = Incurred / Earned Premium
-- Frequency = Claims per 100 policies exposed
-- Severity = Incurred / Claim Count
-- =========================================
CREATE OR REPLACE VIEW dremio.insurance.gold.monthly_loss_metrics AS
WITH monthly_premium AS (
  SELECT
    product_line,
    state,
    premium_month                           AS p_month,
    SUM(earned_premium)                     AS earned_premium,
    COUNT(DISTINCT policy_id)               AS policies_exposed
  FROM dremio.insurance.raw.policy_premiums
  GROUP BY product_line, state, premium_month
),
monthly_claims AS (
  SELECT
    ce.product_line,
    ce.state,
    ce.claim_month                          AS p_month,
    COUNT(*)                                AS claims_count,
    SUM(ce.incurred_amount)                 AS incurred
  FROM dremio.insurance.silver.claims_enriched ce
  GROUP BY ce.product_line, ce.state, ce.claim_month
)
SELECT
  mp.product_line,
  mp.state,
  mp.p_month,
  mp.earned_premium,
  COALESCE(mc.incurred, 0)                                    AS incurred,
  COALESCE(mc.claims_count, 0)                                AS claims_count,
  mp.policies_exposed,
  CASE WHEN mp.earned_premium > 0 
       THEN COALESCE(mc.incurred, 0) / mp.earned_premium 
       ELSE NULL END                                          AS loss_ratio,
  CASE WHEN mp.policies_exposed > 0
       THEN COALESCE(mc.claims_count, 0) * 100.0 / mp.policies_exposed
       ELSE NULL END                                          AS claim_frequency_per_100,
  CASE WHEN COALESCE(mc.claims_count, 0) > 0
       THEN COALESCE(mc.incurred, 0) * 1.0 / mc.claims_count
       ELSE NULL END                                          AS avg_severity
FROM monthly_premium mp
LEFT JOIN monthly_claims mc
  ON mp.product_line = mc.product_line
 AND mp.state        = mc.state
 AND mp.p_month        = mc.p_month;

-- =========================================
-- (Optional) GOLD: Claim cycle time KPI (closed claims only)
-- =========================================
CREATE OR REPLACE VIEW dremio.insurance.gold.monthly_claim_cycle_kpi AS
SELECT
  product_line,
  state,
  claim_month                          AS p_month,
  AVG(claim_cycle_days)                AS avg_claim_cycle_days,
  MIN(claim_cycle_days)                AS min_claim_cycle_days,
  MAX(claim_cycle_days)                AS max_claim_cycle_days,
  COUNT(*)                             AS closed_claims
FROM dremio.insurance.silver.claims_enriched
WHERE status = 'CLOSED'
GROUP BY product_line, state, claim_month;