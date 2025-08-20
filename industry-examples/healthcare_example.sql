-- ================================
-- Namespaces
-- ================================
CREATE FOLDER IF NOT EXISTS dremio.healthcare;
CREATE FOLDER IF NOT EXISTS dremio.healthcare.raw;
CREATE FOLDER IF NOT EXISTS dremio.healthcare.silver;
CREATE FOLDER IF NOT EXISTS dremio.healthcare.gold;

-- ================================
-- BRONZE: Raw encounters (admissions/visits)
-- ================================
CREATE TABLE IF NOT EXISTS dremio.healthcare.raw.encounters (
  admission_id       BIGINT,
  patient_id         VARCHAR,
  encounter_type     VARCHAR,          -- INPATIENT / OUTPATIENT
  admit_ts           TIMESTAMP,
  discharge_ts       TIMESTAMP,        -- may be NULL for same-day/outpatient
  department         VARCHAR,          -- MED, SURG, CARD, etc.
  primary_dx_icd10   VARCHAR,          -- e.g., E11.9
  payer              VARCHAR
)
PARTITION BY (DAY(admit_ts));

-- Sample data
INSERT INTO dremio.healthcare.raw.encounters
(admission_id, patient_id, encounter_type, admit_ts, discharge_ts, department, primary_dx_icd10, payer) VALUES
  (2001, 'P001', 'INPATIENT', TIMESTAMP '2025-08-10 08:00:00', TIMESTAMP '2025-08-12 12:00:00', 'MED',  'E11.9',  'COMMERCIAL'),
  (2002, 'P002', 'OUTPATIENT',TIMESTAMP '2025-08-18 09:30:00', NULL,                               'CARD', 'I10',    'MEDICARE'),
  (2003, 'P001', 'INPATIENT', TIMESTAMP '2025-08-25 08:00:00', TIMESTAMP '2025-08-27 10:00:00', 'MED',  'E11.65','COMMERCIAL'),
  (2004, 'P003', 'INPATIENT', TIMESTAMP '2025-08-19 14:10:00', TIMESTAMP '2025-08-20 11:15:00', 'SURG', 'K35.80','COMMERCIAL'),
  (2005, 'P004', 'OUTPATIENT',TIMESTAMP '2025-08-20 10:00:00', NULL,                               'MED',  'E11.9',  'MEDICAID');

-- ================================
-- BRONZE: Raw lab results
-- ================================
CREATE TABLE IF NOT EXISTS dremio.healthcare.raw.lab_results (
  lab_id        BIGINT,
  patient_id    VARCHAR,
  test_name     VARCHAR,               -- e.g., 'HbA1c', 'LDL'
  result_value  DECIMAL(10,2),
  unit          VARCHAR,               -- '%', 'mg/dL', etc.
  lab_ts        TIMESTAMP,
  abnormal_flag BOOLEAN
)
PARTITION BY (DAY(lab_ts));

-- Sample data
INSERT INTO dremio.healthcare.raw.lab_results
(lab_id, patient_id, test_name, result_value, unit, lab_ts, abnormal_flag) VALUES
  (3001, 'P001', 'HbA1c', 7.80, '%',     TIMESTAMP '2025-07-20 10:00:00', TRUE),
  (3002, 'P002', 'HbA1c', 6.50, '%',     TIMESTAMP '2025-06-01 09:00:00', FALSE),
  (3003, 'P003', 'LDL',   140 , 'mg/dL', TIMESTAMP '2025-08-18 08:30:00', TRUE),
  (3004, 'P004', 'HbA1c', 8.10, '%',     TIMESTAMP '2025-08-19 13:45:00', TRUE),
  (3005, 'P001', 'HbA1c', 7.40, '%',     TIMESTAMP '2025-08-26 07:50:00', TRUE),
  (3006, 'P003', 'HbA1c', 5.80, '%',     TIMESTAMP '2025-05-10 11:00:00', FALSE); -- outside 90d for P003

-- ================================
-- SILVER: Encounters joined to labs within a 90-day window
-- (DATEDIFF uses Dremio syntax: DATEDIFF(endDate, startDate))
-- ================================
CREATE OR REPLACE VIEW dremio.healthcare.silver.encounters_lab_enriched AS
SELECT
  e.admission_id,
  e.patient_id,
  e.encounter_type,
  TO_DATE(e.admit_ts)                                        AS admit_date,
  e.admit_ts,
  COALESCE(e.discharge_ts, e.admit_ts)                       AS discharge_ts,
  e.department,
  e.primary_dx_icd10,
  e.payer,
  r.lab_id,
  r.test_name,
  r.result_value,
  r.unit,
  r.lab_ts,
  r.abnormal_flag,
  CASE WHEN r.test_name = 'HbA1c' THEN 1 ELSE 0 END          AS is_hba1c_flag,
  -- Dremio DATEDIFF(endDate, startDate) → integer days
  DATEDIFF(COALESCE(e.discharge_ts, e.admit_ts), e.admit_ts) AS los_days
FROM dremio.healthcare.raw.encounters e
LEFT JOIN dremio.healthcare.raw.lab_results r
  ON e.patient_id = r.patient_id
 AND r.lab_ts BETWEEN (e.admit_ts - INTERVAL '90' DAY)
                   AND COALESCE(e.discharge_ts, e.admit_ts);

-- ================================
-- GOLD: Diabetes quality KPI (unchanged)
-- ================================
CREATE OR REPLACE VIEW dremio.healthcare.gold.hba1c_compliance_by_dept_day AS
WITH per_encounter AS (
  SELECT
    admission_id,
    department,
    admit_date,
    primary_dx_icd10,
    MAX(CASE WHEN test_name = 'HbA1c' THEN 1 ELSE 0 END) AS has_hba1c_90d
  FROM dremio.healthcare.silver.encounters_lab_enriched
  GROUP BY admission_id, department, admit_date, primary_dx_icd10
)
SELECT
  department,
  admit_date,
  SUM(CASE WHEN primary_dx_icd10 LIKE 'E11%' THEN 1 ELSE 0 END)                                        AS diabetic_encounters,
  SUM(CASE WHEN primary_dx_icd10 LIKE 'E11%' AND has_hba1c_90d = 1 THEN 1 ELSE 0 END)                  AS encounters_with_hba1c,
  CASE 
    WHEN SUM(CASE WHEN primary_dx_icd10 LIKE 'E11%' THEN 1 ELSE 0 END) > 0
    THEN SUM(CASE WHEN primary_dx_icd10 LIKE 'E11%' AND has_hba1c_90d = 1 THEN 1 ELSE 0 END) * 1.0
         / SUM(CASE WHEN primary_dx_icd10 LIKE 'E11%' THEN 1 ELSE 0 END)
    ELSE NULL
  END AS hba1c_compliance_rate
FROM per_encounter
GROUP BY department, admit_date;

-- (Optional) GOLD: ALOS by department/day (uses los_days computed with Dremio DATEDIFF)
CREATE OR REPLACE VIEW dremio.healthcare.gold.alos_by_dept_day AS
SELECT
  department,
  admit_date,
  AVG(los_days) AS avg_los_days
FROM (
  SELECT DISTINCT admission_id, department, admit_date, los_days
  FROM dremio.healthcare.silver.encounters_lab_enriched
)
GROUP BY department, admit_date;