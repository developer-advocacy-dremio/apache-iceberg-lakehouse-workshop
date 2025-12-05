-- ============================================================
-- NAMESPACE SETUP
-- ============================================================
-- Each modeling layer deserves its own folder to promote clear lineage,
-- governance, and consistent team patterns. This aligns with best practices
-- for "early structure in the data pipeline." (Transformations Best Practices)

CREATE FOLDER IF NOT EXISTS dremio.clinicdata;
CREATE FOLDER IF NOT EXISTS dremio.clinicdata.raw;
CREATE FOLDER IF NOT EXISTS dremio.clinicdata.silver;
CREATE FOLDER IF NOT EXISTS dremio.clinicdata.gold;

-- ============================================================
-- RAW LAYER OVERVIEW
-- ============================================================
-- Raw datasets represent the closest form to source data.
-- We intentionally DO NOT clean or filter here.
-- Instead, we keep all fields intact, including:
--   - Nulls
--   - Duplicates
--   - Bad / unneeded rows
--
-- Why? 
--   * Raw = the immutable historical truth.
--   * Cleaning & filtering happens in Silver, ensuring reproducibility and auditability.
--   * This separation reduces maintenance complexity and keeps transformations simple.
--     (Best practice: "Simple transformation logic lowers maintenance effort.")

-- ============================================================
-- RAW PATIENT ENCOUNTERS TABLE
-- ============================================================
-- This table contains intentional data quality issues:
--   - Duplicate encounter rows
--   - Null department values
--   - Rows with invalid encounter types (to be filtered later)
--   - Null patient_ids
-- 
-- Partitioning on DAY(admit_ts) helps accelerate downstream reads,
-- even though raw data is kept as-is.

CREATE TABLE IF NOT EXISTS dremio.clinicdata.raw.encounters (
    encounter_id     BIGINT,
    patient_id       VARCHAR,
    encounter_type   VARCHAR,     -- expected: INPATIENT, OUTPATIENT … but raw may contain garbage values
    admit_ts         TIMESTAMP,
    discharge_ts     TIMESTAMP,
    department       VARCHAR,
    primary_dx       VARCHAR,     -- e.g., ICD-10 codes
    payer            VARCHAR
)
PARTITION BY (DAY(admit_ts));

-- -------------------------------------------------------------
-- SAMPLE RAW DATA WITH INTENTIONAL PROBLEMS
-- -------------------------------------------------------------
INSERT INTO dremio.clinicdata.raw.encounters
(encounter_id, patient_id, encounter_type, admit_ts, discharge_ts, department, primary_dx, payer)
VALUES
  -- VALID CLEAN RECORDS
  (1001, 'A001', 'INPATIENT', TIMESTAMP '2025-02-10 09:00:00', TIMESTAMP '2025-02-12 13:00:00', 'CARD', 'I10', 'MEDICARE'),
  (1002, 'A002', 'OUTPATIENT', TIMESTAMP '2025-02-14 08:30:00', NULL, 'ORTHO', 'M17.0', 'COMMERCIAL'),

  -- DUPLICATE OF 1001 — to show dedupe logic later
  (1001, 'A001', 'INPATIENT', TIMESTAMP '2025-02-10 09:00:00', TIMESTAMP '2025-02-12 13:00:00', 'CARD', 'I10', 'MEDICARE'),

  -- NULL department — will be cleaned or flagged downstream
  (1003, 'A003', 'INPATIENT', TIMESTAMP '2025-02-15 11:00:00', TIMESTAMP '2025-02-16 10:00:00', NULL, 'E11.9', 'MEDICAID'),

  -- INVALID encounter_type — should be filtered out in Silver Filtering
  (1004, 'A004', 'UNKNOWN_TYPE', TIMESTAMP '2025-02-17 10:15:00', TIMESTAMP '2025-02-17 17:20:00', 'NEURO', 'G44.1', 'COMMERCIAL'),

  -- NULL patient_id — bad data that should not appear in analytics
  (1005, NULL, 'OUTPATIENT', TIMESTAMP '2025-02-20 09:45:00', NULL, 'CARD', 'I10', 'MEDICARE');


-- ============================================================
-- RAW LAB RESULTS TABLE
-- ============================================================
-- Includes:
--   - Duplicate lab_id rows
--   - Null result_value
--   - Tests outside clinical time window (filtered in Silver later)
--   - Rows for patients that don't exist in encounters (to illustrate left join nulls)

CREATE TABLE IF NOT EXISTS dremio.clinicdata.raw.lab_results (
    lab_id        BIGINT,
    patient_id    VARCHAR,
    test_name     VARCHAR,
    result_value  DECIMAL(10,2),
    unit          VARCHAR,
    lab_ts        TIMESTAMP,
    abnormal_flag BOOLEAN
)
PARTITION BY (DAY(lab_ts));

-- -------------------------------------------------------------
-- SAMPLE RAW LAB DATA WITH INTENTIONAL PROBLEMS
-- -------------------------------------------------------------
INSERT INTO dremio.clinicdata.raw.lab_results
(lab_id, patient_id, test_name, result_value, unit, lab_ts, abnormal_flag)
VALUES
  -- VALID RECORD
  (2001, 'A001', 'HbA1c', 7.8, '%', TIMESTAMP '2025-02-01 10:00:00', TRUE),

  -- DUPLICATE OF 2001
  (2001, 'A001', 'HbA1c', 7.8, '%', TIMESTAMP '2025-02-01 10:00:00', TRUE),

  -- NULL result_value — will require cleaning rules
  (2002, 'A002', 'LDL', NULL, 'mg/dL', TIMESTAMP '2025-02-10 07:45:00', FALSE),

  -- Outside 90-day window (as an example for downstream filtering)
  (2003, 'A003', 'HbA1c', 6.2, '%', TIMESTAMP '2024-10-01 09:30:00', FALSE),

  -- Patient not present in encounters — join tests downstream
  (2004, 'A999', 'CRP', 5.0, 'mg/L', TIMESTAMP '2025-02-18 16:00:00', TRUE),

  -- Invalid test_name (simulating noise)
  (2005, 'A004', '???', 1.2, 'units', TIMESTAMP '2025-02-17 14:30:00', FALSE);

-- ============================================================
-- SILVER CLEANING VIEW: encounters_cleaned
-- ============================================================
-- Purpose:
--   - Remove duplicates
--   - Enforce required field presence (patient_id, encounter_id)
--   - Normalize column names and apply a consistent projection
--
-- Best Practices Referenced:
--   * Narrow projection: Select only columns required downstream
--   * Avoid SELECT * — reduces scanned bytes and improves performance
--     (Transformations Best Practices, funnel diagram)
--   * Keep transformation logic simple and reusable

CREATE OR REPLACE VIEW dremio.clinicdata.silver.encounters_cleaned AS
WITH deduped AS (
    SELECT
        encounter_id,
        patient_id,
        encounter_type,
        admit_ts,
        discharge_ts,
        department,
        primary_dx,
        payer,
        ROW_NUMBER() OVER (
            PARTITION BY encounter_id
            ORDER BY admit_ts DESC
        ) AS rn
    FROM dremio.clinicdata.raw.encounters
)
SELECT
    encounter_id,
    patient_id,
    encounter_type,
    admit_ts,
    discharge_ts,
    department,
    primary_dx,
    payer
FROM deduped
WHERE rn = 1
  AND patient_id IS NOT NULL        -- ensures analytic integrity
  AND encounter_id IS NOT NULL;


-- ============================================================
-- SILVER CLEANING VIEW: labs_cleaned
-- ============================================================
-- Purpose:
--   - Remove duplicate lab_id rows
--   - Filter out null result_value where invalid for analytics
--   - Normalize structure for later joins
--
-- Best Practices:
--   * Early pruning reduces downstream join volume
--   * Clean logic kept in its own view for reusability across gold models

CREATE OR REPLACE VIEW dremio.clinicdata.silver.labs_cleaned AS
WITH deduped AS (
    SELECT
        lab_id,
        patient_id,
        test_name,
        result_value,
        unit,
        lab_ts,
        abnormal_flag,
        ROW_NUMBER() OVER (
            PARTITION BY lab_id
            ORDER BY lab_ts DESC
        ) AS rn
    FROM dremio.clinicdata.raw.lab_results
)
SELECT
    lab_id,
    patient_id,
    test_name,
    result_value,
    unit,
    lab_ts,
    abnormal_flag
FROM deduped
WHERE rn = 1
  AND patient_id IS NOT NULL;      -- drop unjoinable noise


-- ============================================================
-- SILVER FILTER VIEW: encounters_filtered
-- ============================================================
-- Purpose:
--   - Remove invalid encounter types introduced in the raw layer
--   - Keep only clinically valid rows for downstream analytics
--
-- Best Practices:
--   * “Filter early to trim large tables before joins”
--     (Best Practices slide: Reduce scanned data before joins)
--   * Simple, domain-driven rules in a reusable layer

CREATE OR REPLACE VIEW dremio.clinicdata.silver.encounters_filtered AS
SELECT
    encounter_id,
    patient_id,
    encounter_type,
    admit_ts,
    discharge_ts,
    department,
    primary_dx,
    payer
FROM dremio.clinicdata.silver.encounters_cleaned
WHERE encounter_type IN ('INPATIENT', 'OUTPATIENT');

-- ============================================================
-- SILVER FILTER VIEW: labs_filtered
-- ============================================================
-- Purpose:
--   - Remove invalid test names
--   - Drop unusable results (missing values, malformed entries)
--
-- Best Practices:
--   * Early removal of low-quality data improves join performance
--   * Keep filtering minimal and domain-specific

CREATE OR REPLACE VIEW dremio.clinicdata.silver.labs_filtered AS
SELECT
    lab_id,
    patient_id,
    test_name,
    result_value,
    unit,
    lab_ts,
    abnormal_flag
FROM dremio.clinicdata.silver.labs_cleaned
WHERE
    result_value IS NOT NULL
    AND test_name NOT IN ('???');     -- domain-specific noise pattern

-- ============================================================
-- SILVER JOIN VIEW: encounters_labs_enriched
-- ============================================================
-- Purpose:
--   - Join encounters + labs via patient_id
--   - Include labs occurring within a 90-day window before admission
--   - Add useful derived fields
--
-- Dremio-specific syntax:
--   * DATEDIFF(end, start) → integer days
--   * Use COALESCE to cover missing discharge timestamps

CREATE OR REPLACE VIEW dremio.clinicdata.silver.encounters_labs_enriched AS
SELECT
    e.encounter_id,
    e.patient_id,
    e.encounter_type,
    e.admit_ts,
    e.discharge_ts,
    TO_DATE(e.admit_ts) AS admit_date,
    e.department,
    e.primary_dx,
    e.payer,

    -- lab fields
    l.lab_id,
    l.test_name,
    l.result_value,
    l.unit,
    l.lab_ts,
    l.abnormal_flag,

    -- derived metrics
    CASE WHEN l.test_name = 'HbA1c' THEN 1 ELSE 0 END AS is_hba1c_flag,
    DATEDIFF(COALESCE(e.discharge_ts, e.admit_ts), e.admit_ts) AS los_days

FROM dremio.clinicdata.silver.encounters_filtered e
LEFT JOIN dremio.clinicdata.silver.labs_filtered l
  ON e.patient_id = l.patient_id
 AND l.lab_ts BETWEEN (e.admit_ts - INTERVAL '90' DAY)
                   AND COALESCE(e.discharge_ts, e.admit_ts);

-- ============================================================
-- GOLD VIEW 1: hba1c_compliance
-- ============================================================
-- Purpose:
--   - Clinical quality metric for diabetic encounters
--   - Measures whether an HbA1c test was performed within a visit window
--
-- Best Practices:
--   - Derived metrics built from standardized Silver model
--   - Avoid SELECT *, use clean projection
--   - Keep logic easy for BI tools to consume

CREATE OR REPLACE VIEW dremio.clinicdata.gold.hba1c_compliance AS
WITH per_encounter AS (
    SELECT
        encounter_id,
        patient_id,
        admit_date,
        department,
        primary_dx,
        MAX(CASE WHEN test_name = 'HbA1c' THEN 1 ELSE 0 END) AS has_hba1c
    FROM dremio.clinicdata.silver.encounters_labs_enriched
    GROUP BY encounter_id, patient_id, admit_date, department, primary_dx
)
SELECT
    department,
    admit_date,
    COUNT(*) FILTER (WHERE primary_dx LIKE 'E11%') AS diabetic_encounters,
    COUNT(*) FILTER (WHERE primary_dx LIKE 'E11%' AND has_hba1c = 1) AS diabetic_with_hba1c,
    CASE
        WHEN COUNT(*) FILTER (WHERE primary_dx LIKE 'E11%') > 0
        THEN (
            COUNT(*) FILTER (WHERE primary_dx LIKE 'E11%' AND has_hba1c = 1)
            * 1.0 /
            COUNT(*) FILTER (WHERE primary_dx LIKE 'E11%')
        )
    END AS hba1c_compliance_rate
FROM per_encounter
GROUP BY department, admit_date;

-- ============================================================
-- GOLD VIEW 2: alos_by_department
-- ============================================================
-- Purpose:
--   - Operational metric for hospital/clinic capacity planning
--   - Uses pre-cleaned Silver model for consistent LOS calculation
--
-- Best Practice:
--   - LOS derived in Silver → avoids duplicate logic in BI layer

CREATE OR REPLACE VIEW dremio.clinicdata.gold.alos_by_department AS
SELECT
    department,
    admit_date,
    AVG(los_days) AS avg_los_days
FROM (
    SELECT DISTINCT       -- ensures no duplication from lab joins
        encounter_id,
        department,
        admit_date,
        los_days
    FROM dremio.clinicdata.silver.encounters_labs_enriched
)
GROUP BY department, admit_date;

-- ============================================================
-- GOLD VIEW 3: payer_mix_summary
-- ============================================================
-- Purpose:
--   - Financial analytics: payer mix influences reimbursement & revenue
--   - Cleanly grouped for reporting dashboards
--
-- Best Practices:
--   - Leverages curated Silver model for consistent encounter attributes
--   - Avoid SELECT *; create narrow aggregated result

CREATE OR REPLACE VIEW dremio.clinicdata.gold.payer_mix_summary AS
SELECT
    department,
    encounter_type,
    payer,
    COUNT(DISTINCT encounter_id) AS encounter_count
FROM dremio.clinicdata.silver.encounters_labs_enriched
GROUP BY department, encounter_type, payer;

