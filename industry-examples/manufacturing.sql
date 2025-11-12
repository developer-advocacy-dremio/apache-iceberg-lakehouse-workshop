-- =========================================
-- MANUFACTURING: IoT + Maintenance Pipeline
-- Namespace: dremio.manufacturing
-- Layers: raw → silver (CTAS) → gold (CTAS)
-- Also: quality views for raw/silver/gold
-- =========================================

-- -------------------------------
-- Namespaces
-- -------------------------------
CREATE FOLDER IF NOT EXISTS dremio.manufacturing;
CREATE FOLDER IF NOT EXISTS dremio.manufacturing.raw;
CREATE FOLDER IF NOT EXISTS dremio.manufacturing.silver;
CREATE FOLDER IF NOT EXISTS dremio.manufacturing.gold;
CREATE FOLDER IF NOT EXISTS dremio.manufacturing.quality;

-- ============================================================
-- RAW LAYER (seed data)
-- ============================================================

-- Machines master
CREATE TABLE IF NOT EXISTS dremio.manufacturing.raw.machines (
  machine_id   VARCHAR,
  site         VARCHAR,     -- e.g., PLANT_A
  line         VARCHAR,     -- e.g., LINE_1
  install_ts   TIMESTAMP,
  status       VARCHAR      -- ACTIVE / INACTIVE / MAINT
);

INSERT INTO dremio.manufacturing.raw.machines
(machine_id, site, line, install_ts, status) VALUES
  ('M001','PLANT_A','LINE_1', TIMESTAMP '2025-06-15 08:00:00','ACTIVE'),
  ('M002','PLANT_A','LINE_2', TIMESTAMP '2025-07-01 08:00:00','ACTIVE'),
  ('M003','PLANT_B','LINE_3', TIMESTAMP '2025-07-20 08:00:00','MAINT');

-- High-volume IoT sensor readings
CREATE TABLE IF NOT EXISTS dremio.manufacturing.raw.sensor_readings (
  reading_id    BIGINT,
  machine_id    VARCHAR,
  sensor_type   VARCHAR,     -- TEMP / VIB / RPM
  reading_ts    TIMESTAMP,
  "value"         DECIMAL(10,3),
  unit          VARCHAR
)
PARTITION BY (DAY(reading_ts));

INSERT INTO dremio.manufacturing.raw.sensor_readings
(reading_id, machine_id, sensor_type, reading_ts, "value", unit) VALUES
  (9001,'M001','TEMP',TIMESTAMP '2025-08-10 10:00:00', 68.2, 'C'),
  (9002,'M001','VIB', TIMESTAMP '2025-08-10 10:00:05',  3.2, 'mm/s'),
  (9003,'M002','TEMP',TIMESTAMP '2025-08-10 10:01:00', 72.5, 'C'),
  (9004,'M002','RPM', TIMESTAMP '2025-08-10 10:01:10',1500 , 'rpm'),
  (9005,'M003','TEMP',TIMESTAMP '2025-08-10 10:02:00',120.0, 'C'), -- high
  (9006,'M001','RPM', TIMESTAMP '2025-08-10 10:03:00',  50 , 'rpm'); -- low-ish

-- Work orders (failures/maintenance)
CREATE TABLE IF NOT EXISTS dremio.manufacturing.raw.work_orders (
  wo_id        BIGINT,
  machine_id   VARCHAR,
  open_ts      TIMESTAMP,
  close_ts     TIMESTAMP,       -- may be NULL if still open
  wo_type      VARCHAR,         -- FAIL, PREVENTIVE, CALIBRATE
  part         VARCHAR,
  qty          INT,
  status       VARCHAR          -- OPEN / CLOSED / CANCELED
)
PARTITION BY (DAY(open_ts));

INSERT INTO dremio.manufacturing.raw.work_orders
(wo_id, machine_id, open_ts, close_ts, wo_type, part, qty, status) VALUES
  (7001,'M001',TIMESTAMP '2025-08-09 09:00:00',TIMESTAMP '2025-08-09 12:30:00','FAIL','BRG-9',1,'CLOSED'),
  (7002,'M002',TIMESTAMP '2025-08-10 07:30:00',NULL,'PREVENTIVE','OIL-5W',1,'OPEN'),
  (7003,'M003',TIMESTAMP '2025-08-08 14:00:00',TIMESTAMP '2025-08-08 18:00:00','CALIBRATE','CAL-KIT',1,'CLOSED');

-- ============================================================
-- RAW QUALITY / HEALTH CHECK VIEWS
-- ============================================================

-- Nulls, out-of-range, duplicate reading_ids (by simple rule-of-thumb bounds)
CREATE OR REPLACE VIEW dremio.manufacturing.quality.raw_sensor_health AS
WITH bounds AS (
  SELECT 'TEMP' AS sensor_type, -40.0 AS min_v, 200.0 AS max_v UNION ALL
  SELECT 'VIB' ,  0.0 , 50.0  UNION ALL
  SELECT 'RPM' ,  0.0 , 10000.0
),
dups AS (
  SELECT reading_id, COUNT(*) AS cnt
  FROM dremio.manufacturing.raw.sensor_readings
  GROUP BY reading_id
  HAVING COUNT(*) > 1
)
SELECT
  r.reading_id,
  r.machine_id,
  r.sensor_type,
  r.reading_ts,
  r."value",
  CASE WHEN r.machine_id IS NULL OR r.sensor_type IS NULL OR r.reading_ts IS NULL THEN 1 ELSE 0 END AS has_nulls,
  CASE WHEN r."value" < b.min_v OR r."value" > b.max_v THEN 1 ELSE 0 END AS out_of_range,
  CASE WHEN d.reading_id IS NOT NULL THEN 1 ELSE 0 END AS is_duplicate_id
FROM dremio.manufacturing.raw.sensor_readings r
LEFT JOIN bounds b ON r.sensor_type = b.sensor_type
LEFT JOIN dups d   ON r.reading_id = d.reading_id;

-- Work order integrity: bad time order, non-positive qty, unexpected status
CREATE OR REPLACE VIEW dremio.manufacturing.quality.raw_wo_health AS
SELECT
  wo_id,
  machine_id,
  open_ts,
  close_ts,
  qty,
  status,
  CASE WHEN close_ts IS NOT NULL AND close_ts < open_ts THEN 1 ELSE 0 END AS invalid_time_range,
  CASE WHEN qty IS NULL OR qty <= 0 THEN 1 ELSE 0 END AS bad_qty,
  CASE WHEN UPPER(TRIM(status)) NOT IN ('OPEN','CLOSED','CANCELED') THEN 1 ELSE 0 END AS bad_status
FROM dremio.manufacturing.raw.work_orders;

-- ============================================================
-- SILVER LAYER (physical CTAS)
-- ============================================================

-- Step 1: Machines normalized
DROP TABLE IF EXISTS dremio.manufacturing.silver.machines_current;
CREATE TABLE dremio.manufacturing.silver.machines_current AS
SELECT
  TRIM(machine_id) AS machine_id,
  UPPER(TRIM(site)) AS site,
  UPPER(TRIM(line)) AS line,
  install_ts,
  UPPER(TRIM(status)) AS status
FROM dremio.manufacturing.raw.machines;

-- Step 2: Clean sensor readings, basic flags + reading_date
DROP TABLE IF EXISTS dremio.manufacturing.silver.sensor_clean;
CREATE TABLE dremio.manufacturing.silver.sensor_clean
PARTITION BY (reading_date)
AS
WITH bounds AS (
  SELECT 'TEMP' AS sensor_type, -40.0 AS min_v, 200.0 AS max_v UNION ALL
  SELECT 'VIB' ,  0.0 , 50.0  UNION ALL
  SELECT 'RPM' ,  0.0 , 10000.0
)
SELECT
  r.reading_id,
  r.machine_id,
  UPPER(TRIM(r.sensor_type)) AS sensor_type,
  r.reading_ts,
  r."value",
  r.unit,
  TO_DATE(r.reading_ts) AS reading_date,
  CASE WHEN r."value" < b.min_v OR r."value" > b.max_v THEN 1 ELSE 0 END AS out_of_range_flag
FROM dremio.manufacturing.raw.sensor_readings r
LEFT JOIN bounds b ON UPPER(TRIM(r.sensor_type)) = b.sensor_type;

-- Step 3: Work orders normalized, duration_days (coarse)
DROP TABLE IF EXISTS dremio.manufacturing.silver.wo_clean;
CREATE TABLE dremio.manufacturing.silver.wo_clean
PARTITION BY (open_date)
AS
SELECT
  wo_id,
  machine_id,
  open_ts,
  close_ts,
  TO_DATE(open_ts) AS open_date,
  UPPER(TRIM(wo_type)) AS wo_type,
  UPPER(TRIM(status)) AS status,
  part,
  qty,
  -- Dremio DATEDIFF(end, start) returns days (integer)
  DATEDIFF(COALESCE(close_ts, open_ts), open_ts) AS duration_days
FROM dremio.manufacturing.raw.work_orders;

-- Step 4: Sensor readings within open-close WO windows (per reading)
DROP TABLE IF EXISTS dremio.manufacturing.silver.sensor_within_wo;
CREATE TABLE dremio.manufacturing.silver.sensor_within_wo
PARTITION BY (reading_date)
DISTRIBUTE BY (machine_id)
AS
SELECT
  s.reading_id,
  s.machine_id,
  s.sensor_type,
  s.reading_ts,
  s.reading_date,
  s."value",
  s.unit,
  s.out_of_range_flag,
  w.wo_id,
  w.wo_type,
  w.status AS wo_status,
  w.open_ts,
  w.close_ts
FROM dremio.manufacturing.silver.sensor_clean s
LEFT JOIN dremio.manufacturing.silver.wo_clean w
  ON s.machine_id = w.machine_id
 AND s.reading_ts BETWEEN w.open_ts AND COALESCE(w.close_ts, s.reading_ts);

-- Step 5: Daily machine sensor aggregates
DROP TABLE IF EXISTS dremio.manufacturing.silver.machine_sensor_daily;
CREATE TABLE dremio.manufacturing.silver.machine_sensor_daily
PARTITION BY (reading_date)
AS
SELECT
  machine_id,
  reading_date,
  AVG(CASE WHEN sensor_type = 'TEMP' THEN "value" END) AS avg_temp_c,
  AVG(CASE WHEN sensor_type = 'VIB'  THEN "value" END) AS avg_vib_mms,
  AVG(CASE WHEN sensor_type = 'RPM'  THEN "value" END) AS avg_rpm,
  SUM(CASE WHEN out_of_range_flag = 1 THEN 1 ELSE 0 END) AS out_of_range_cnt
FROM dremio.manufacturing.silver.sensor_clean
GROUP BY machine_id, reading_date;

-- ============================================================
-- SILVER QUALITY / HEALTH CHECK VIEWS
-- ============================================================

-- FK violations: sensor readings referencing unknown machines
CREATE OR REPLACE VIEW dremio.manufacturing.quality.silver_fk_violations AS
SELECT s.*
FROM dremio.manufacturing.silver.sensor_clean s
LEFT JOIN dremio.manufacturing.silver.machines_current m
  ON s.machine_id = m.machine_id
WHERE m.machine_id IS NULL;

-- Temporal sanity: readings before install or far after (±30d from install)
CREATE OR REPLACE VIEW dremio.manufacturing.quality.silver_temporal_health AS
SELECT
  s.machine_id,
  s.reading_id,
  s.reading_ts,
  m.install_ts,
  CASE
    WHEN s.reading_ts < m.install_ts THEN 1
    WHEN s.reading_ts > (m.install_ts + INTERVAL '1' YEAR) THEN 1
    ELSE 0
  END AS temporal_issue_flag
FROM dremio.manufacturing.silver.sensor_clean s
JOIN dremio.manufacturing.silver.machines_current m
  ON s.machine_id = m.machine_id;

-- Long-running work orders (>30d)
CREATE OR REPLACE VIEW dremio.manufacturing.quality.silver_wo_duration_health AS
SELECT
  wo_id,
  machine_id,
  open_ts,
  close_ts,
  duration_days,
  CASE WHEN duration_days > 30 THEN 1 ELSE 0 END AS over_30d_flag
FROM dremio.manufacturing.silver.wo_clean;

-- ============================================================
-- GOLD LAYER (physical CTAS)
-- ============================================================

-- GOLD 1: Daily production/condition snapshot per machine
DROP TABLE IF EXISTS dremio.manufacturing.gold.machine_daily_snapshot;
CREATE TABLE dremio.manufacturing.gold.machine_daily_snapshot
PARTITION BY (snapshot_date)
AS
WITH wo_day AS (
  SELECT
    machine_id,
    TO_DATE(open_ts) AS snapshot_date,
    COUNT(*) AS wo_opened,
    SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END) AS wo_closed
  FROM dremio.manufacturing.silver.wo_clean
  GROUP BY machine_id, TO_DATE(open_ts)
)
SELECT
  d.machine_id,
  d.reading_date AS snapshot_date,
  d.avg_temp_c,
  d.avg_vib_mms,
  d.avg_rpm,
  d.out_of_range_cnt,
  COALESCE(w.wo_opened,0) AS wo_opened,
  COALESCE(w.wo_closed,0) AS wo_closed
FROM dremio.manufacturing.silver.machine_sensor_daily d
LEFT JOIN wo_day w
  ON d.machine_id = w.machine_id
 AND d.reading_date = w.snapshot_date;

-- GOLD 2: MTBF (days) per machine based on FAIL work orders
DROP TABLE IF EXISTS dremio.manufacturing.gold.mtbf_days;
CREATE TABLE dremio.manufacturing.gold.mtbf_days AS
WITH fails AS (
  SELECT
    machine_id,
    open_ts,
    TO_DATE(open_ts) AS open_date,
    ROW_NUMBER() OVER (PARTITION BY machine_id ORDER BY open_ts) AS rn
  FROM dremio.manufacturing.silver.wo_clean
  WHERE wo_type = 'FAIL'
),
pairs AS (
  SELECT
    f1.machine_id,
    f1.open_date AS this_fail_date,
    f2.open_date AS next_fail_date
  FROM fails f1
  LEFT JOIN fails f2
    ON f1.machine_id = f2.machine_id
   AND f2.rn = f1.rn + 1
)
SELECT
  machine_id,
  this_fail_date,
  next_fail_date,
  CASE
    WHEN next_fail_date IS NOT NULL
    THEN DATEDIFF(next_fail_date, this_fail_date)
    ELSE NULL
  END AS mtbf_days
FROM pairs;

-- GOLD 3: Site/line daily KPIs
DROP TABLE IF EXISTS dremio.manufacturing.gold.site_line_daily_kpis;
CREATE TABLE dremio.manufacturing.gold.site_line_daily_kpis
PARTITION BY (kpi_date)
AS
SELECT
  m.site,
  m.line,
  s.snapshot_date AS kpi_date,
  COUNT(DISTINCT s.machine_id) AS machines_reporting,
  AVG(s.avg_temp_c)            AS avg_temp_c,
  AVG(s.avg_vib_mms)           AS avg_vib_mms,
  AVG(s.avg_rpm)               AS avg_rpm,
  SUM(s.out_of_range_cnt)      AS out_of_range_events,
  SUM(s.wo_opened)             AS wo_opened,
  SUM(s.wo_closed)             AS wo_closed
FROM dremio.manufacturing.gold.machine_daily_snapshot s
JOIN dremio.manufacturing.silver.machines_current m
  ON s.machine_id = m.machine_id
GROUP BY m.site, m.line, s.snapshot_date;

-- ============================================================
-- GOLD QUALITY / HEALTH CHECK VIEWS
-- ============================================================

-- Reconciliation: gold machine_daily_snapshot vs silver aggregates
CREATE OR REPLACE VIEW dremio.manufacturing.quality.gold_reconciliation AS
WITH silver_calc AS (
  SELECT
    machine_id,
    reading_date,
    COUNT(*) FILTER (WHERE out_of_range_flag = 1) AS silver_out_of_range_cnt
  FROM dremio.manufacturing.silver.sensor_clean
  GROUP BY machine_id, reading_date
)
SELECT
  g.machine_id,
  g.snapshot_date,
  g.out_of_range_cnt AS gold_cnt,
  s.silver_out_of_range_cnt AS silver_cnt,
  (g.out_of_range_cnt - s.silver_out_of_range_cnt) AS diff
FROM dremio.manufacturing.gold.machine_daily_snapshot g
JOIN silver_calc s
  ON g.machine_id = s.machine_id
 AND g.snapshot_date = s.reading_date
WHERE (g.out_of_range_cnt <> s.silver_out_of_range_cnt);

-- KPI null checks
CREATE OR REPLACE VIEW dremio.manufacturing.quality.gold_kpi_nulls AS
SELECT *
FROM dremio.manufacturing.gold.site_line_daily_kpis
WHERE avg_temp_c IS NULL OR avg_vib_mms IS NULL OR machines_reporting IS NULL;

-- Trend sanity: RPM negative or absurd
CREATE OR REPLACE VIEW dremio.manufacturing.quality.gold_rpm_sanity AS
SELECT
  site,
  line,
  kpi_date,
  avg_rpm,
  CASE WHEN avg_rpm < 0 OR avg_rpm > 10000 THEN 1 ELSE 0 END AS rpm_issue
FROM dremio.manufacturing.gold.site_line_daily_kpis
WHERE avg_rpm < 0 OR avg_rpm > 10000;

-- =========================================
-- Done. Pipeline + health checks created.
--   RAW:    dremio.manufacturing.raw.*
--   SILVER: dremio.manufacturing.silver.*
--   GOLD:   dremio.manufacturing.gold.*
--   QC:     dremio.manufacturing.quality.*
-- =========================================
