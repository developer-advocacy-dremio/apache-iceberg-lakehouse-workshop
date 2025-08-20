-- =========================================
-- Namespaces
-- =========================================
CREATE FOLDER IF NOT EXISTS dremio.gov;
CREATE FOLDER IF NOT EXISTS dremio.gov.raw;
CREATE FOLDER IF NOT EXISTS dremio.gov.silver;
CREATE FOLDER IF NOT EXISTS dremio.gov.gold;

-- =========================================
-- BRONZE: 311 service requests (as-received)
-- =========================================
CREATE TABLE IF NOT EXISTS dremio.gov.raw.requests_311 (
  request_id     BIGINT,
  citizen_id     VARCHAR,
  category       VARCHAR,       -- e.g., "Pothole", "Streetlight", "Trash"
  priority       VARCHAR,       -- e.g., "High", "Medium", "Low"
  description    VARCHAR,
  created_ts     TIMESTAMP,
  zipcode        VARCHAR,
  channel        VARCHAR,       -- e.g., "Web", "Phone", "App"
  sla_days       INTEGER        -- target resolution window in days
)
PARTITION BY (DAY(created_ts));

INSERT INTO dremio.gov.raw.requests_311
(request_id, citizen_id, category, priority, description, created_ts, zipcode, channel, sla_days) VALUES
  (800001, 'C-101', 'Pothole'   , 'High'  , 'Large pothole on 5th Ave'          , TIMESTAMP '2025-08-18 08:05:00', '10001', 'App' , 3),
  (800002, 'C-102', 'Streetlight', 'Medium', 'Streetlight flickering on Main St', TIMESTAMP '2025-08-18 09:22:00', '10002', 'Web' , 5),
  (800003, 'C-103', 'Trash'     , 'Low'   , 'Overflowing bin at corner'         , TIMESTAMP '2025-08-19 10:10:00', '10003', 'Phone', 2),
  (800004, 'C-104', 'Graffiti'  , 'Medium', 'Graffiti on underpass wall'        , TIMESTAMP '2025-08-19 11:45:00', '10001', 'App' , 7),
  (800005, 'C-105', 'Pothole'   , 'High'  , 'Multiple potholes after rain'      , TIMESTAMP '2025-08-20 07:55:00', '10002', 'Web' , 3);

-- =========================================
-- BRONZE: Work orders / field operations
-- Assume one work order per request in this sample
-- =========================================
CREATE TABLE IF NOT EXISTS dremio.gov.raw.work_orders (
  work_order_id  BIGINT,
  request_id     BIGINT,
  department     VARCHAR,       -- e.g., "DOT", "Sanitation", "Public Works"
  status         VARCHAR,       -- "OPEN", "IN_PROGRESS", "CLOSED"
  dispatch_ts    TIMESTAMP,     -- crew dispatched
  start_ts       TIMESTAMP,     -- work began
  complete_ts    TIMESTAMP      -- NULL if still open
)
PARTITION BY (DAY(dispatch_ts));

INSERT INTO dremio.gov.raw.work_orders
(work_order_id, request_id, department, status, dispatch_ts, start_ts, complete_ts) VALUES
  (900001, 800001, 'DOT'         , 'CLOSED'     , TIMESTAMP '2025-08-18 10:00:00', TIMESTAMP '2025-08-18 11:00:00', TIMESTAMP '2025-08-19 15:30:00'),
  (900002, 800002, 'Public Works', 'IN_PROGRESS', TIMESTAMP '2025-08-18 13:15:00', TIMESTAMP '2025-08-19 09:00:00', NULL),
  (900003, 800003, 'Sanitation'  , 'CLOSED'     , TIMESTAMP '2025-08-19 12:00:00', TIMESTAMP '2025-08-19 12:30:00', TIMESTAMP '2025-08-19 16:45:00'),
  (900004, 800004, 'Public Works', 'CLOSED'     , TIMESTAMP '2025-08-20 08:20:00', TIMESTAMP '2025-08-20 09:10:00', TIMESTAMP '2025-08-23 10:00:00'),
  (900005, 800005, 'DOT'         , 'OPEN'       , TIMESTAMP '2025-08-20 09:10:00', NULL                           , NULL);

-- =========================================
-- SILVER: Clean join + SLA/lag signals
-- Uses Dremio DATEDIFF(endDate, startDate) → integer days
-- =========================================
CREATE OR REPLACE VIEW dremio.gov.silver.requests_enriched AS
SELECT
  r.request_id,
  r.citizen_id,
  r.category,
  r.priority,
  r.description,
  r.zipcode,
  r.channel,
  r.sla_days,
  TO_DATE(r.created_ts)                                  AS request_date,
  w.work_order_id,
  w.department,
  w.status,
  TO_DATE(w.dispatch_ts)                                 AS dispatch_date,
  TO_DATE(w.complete_ts)                                 AS complete_date,
  -- Days from creation to dispatch / completion
  DATEDIFF(w.dispatch_ts, r.created_ts)                  AS response_days,
  DATEDIFF(COALESCE(w.complete_ts, w.dispatch_ts), r.created_ts) AS resolution_days_partial,
  -- SLA due timestamp via TIMESTAMPADD for dynamic interval
  TIMESTAMPADD(DAY, r.sla_days, r.created_ts)          AS sla_due_ts,
  -- SLA compliance: completed on or before due
  CASE 
    WHEN w.complete_ts IS NOT NULL 
     AND w.complete_ts <= TIMESTAMPADD(DAY, r.sla_days, r.created_ts)
    THEN 1 ELSE 0 
  END                                                    AS sla_met_flag,
  -- Open/backlog flag
  CASE WHEN w.complete_ts IS NULL THEN 1 ELSE 0 END      AS open_flag
FROM dremio.gov.raw.requests_311 r
LEFT JOIN dremio.gov.raw.work_orders w
  ON r.request_id = w.request_id;

-- =========================================
-- GOLD: Daily KPIs by department & category
-- Answers: "How are we performing vs SLA, and where are backlogs growing?"
-- =========================================
CREATE OR REPLACE VIEW dremio.gov.gold.daily_kpis_by_dept_category AS
SELECT
  department,
  category,
  request_date,
  COUNT(*)                                                  AS requests,
  SUM(CASE WHEN open_flag = 1 THEN 1 ELSE 0 END)           AS open_requests,
  SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END)       AS closed_requests,
  AVG(response_days)                                       AS avg_response_days,
  AVG(resolution_days_partial)                             AS avg_resolution_days,
  AVG(sla_met_flag)                                        AS sla_compliance_rate
FROM dremio.gov.silver.requests_enriched
GROUP BY department, category, request_date;

-- =========================================
-- (Optional) GOLD: Backlog trend by ZIP
-- =========================================
CREATE OR REPLACE VIEW dremio.gov.gold.backlog_by_zip_daily AS
SELECT
  zipcode,
  request_date,
  SUM(open_flag) AS open_requests
FROM dremio.gov.silver.requests_enriched
GROUP BY zipcode, request_date;