-- ====================================================================================
-- DREMIO LOGISTICS EXAMPLE: USING AI FUNCTIONS WITH “UNSTRUCTURED” TEXT IN TABLES
-- ====================================================================================
-- Scenario:
--   You operate a logistics company with many delivery events.
--   Drivers write free-text notes about issues (traffic, weather, access problems).
--   You want to:
--     - Extract structured fields from messy notes           → AI_GENERATE
--     - Classify risk / severity levels                      → AI_CLASSIFY
--     - Generate human-readable summaries for operations     → AI_COMPLETE
--
-- IMPORTANT:
--   • This example assumes NO object storage sources are connected.
--   • We simulate unstructured data by storing text in VARCHAR columns.
--   • In comments, we show how the same patterns would work with LIST_FILES
--     and true unstructured files (PDFs, images, etc.) once sources exist.
--
-- Namespaces follow a bronze/silver/gold pattern:
--   dremio.logistics.raw     = raw / landing tables
--   dremio.logistics.silver  = AI-enriched views
--   dremio.logistics.gold    = business metrics / dashboards
-- ====================================================================================


-- =========================================
-- 1. NAMESPACES
-- =========================================
CREATE FOLDER IF NOT EXISTS dremio.logistics;
CREATE FOLDER IF NOT EXISTS dremio.logistics.raw;
CREATE FOLDER IF NOT EXISTS dremio.logistics.silver;
CREATE FOLDER IF NOT EXISTS dremio.logistics.gold;


-- =========================================
-- 2. BRONZE: RAW DELIVERY EVENTS
-- =========================================
-- This table simulates:
--   • Structured delivery event data
--   • Unstructured driver notes (free-text), as if copied from emails,
--     mobile app forms, or transcribed call center logs.
--
-- In a real deployment:
--   • The free-text could be OCR from PDFs, text from emails,
--     or transcripts from call recordings stored in object storage.
--   • AI functions would process those documents directly via LIST_FILES.
-- =========================================
CREATE TABLE IF NOT EXISTS dremio.logistics.raw.delivery_events (
  delivery_id        BIGINT,
  route_id           VARCHAR,
  driver_id          VARCHAR,
  scheduled_dt       TIMESTAMP,
  actual_dt          TIMESTAMP,
  destination_city   VARCHAR,
  destination_region VARCHAR,
  -- Unstructured / semi-structured text field
  driver_notes       VARCHAR
);

-- Sample data: a mix of issues (traffic, weather, access, customer, mechanical)
DELETE FROM dremio.logistics.raw.delivery_events;  -- for repeatable runs

INSERT INTO dremio.logistics.raw.delivery_events (
  delivery_id, route_id, driver_id, scheduled_dt, actual_dt,
  destination_city, destination_region, driver_notes
) VALUES
  (20001, 'RT-NE-001', 'DRV-101',
   TIMESTAMP '2025-08-18 08:00:00', TIMESTAMP '2025-08-18 09:05:00',
   'Boston', 'NE',
   'Heavy traffic near downtown, about 45 minutes delay. Customer notified and accepted new ETA. No damage.'),
  (20002, 'RT-NE-001', 'DRV-101',
   TIMESTAMP '2025-08-18 11:00:00', TIMESTAMP '2025-08-18 11:10:00',
   'Cambridge', 'NE',
   'On time delivery. Parking a bit tight but no major issue. Customer happy.'),
  (20003, 'RT-MW-004', 'DRV-202',
   TIMESTAMP '2025-08-18 07:30:00', TIMESTAMP '2025-08-18 10:05:00',
   'Chicago', 'MW',
   'Major storm with flooding on I-90. Rerouted through side streets. Shipment arrived wet and one box slightly damaged.'),
  (20004, 'RT-SE-002', 'DRV-303',
   TIMESTAMP '2025-08-18 09:00:00', TIMESTAMP '2025-08-18 12:45:00',
   'Atlanta', 'SE',
   'Gate access code not working. Waited over an hour. Several calls to customer and dispatch to resolve security clearance.'),
  (20005, 'RT-W-010', 'DRV-404',
   TIMESTAMP '2025-08-19 06:00:00', TIMESTAMP '2025-08-19 06:55:00',
   'Los Angeles', 'W',
   'Light traffic. Quick unload. Customer mentioned previous damage issues but this shipment is fine.'),
  (20006, 'RT-W-011', 'DRV-404',
   TIMESTAMP '2025-08-19 08:00:00', TIMESTAMP '2025-08-19 11:30:00',
   'San Diego', 'W',
   'Truck had engine warning light, stopped at service station for inspection. Lost about two hours. Shipment delivered intact.'),
  (20007, 'RT-MW-005', 'DRV-505',
   TIMESTAMP '2025-08-19 09:00:00', TIMESTAMP '2025-08-19 09:50:00',
   'Milwaukee', 'MW',
   'On time. No issues. Customer requested recurring early morning slot.'),
  (20008, 'RT-SE-003', 'DRV-303',
   TIMESTAMP '2025-08-19 13:00:00', TIMESTAMP '2025-08-19 15:40:00',
   'Charlotte', 'SE',
   'Accident on highway caused full closure. Police redirected traffic. Shipment late and customer upset, asked for discount.');


-- =========================================
-- 3. SILVER: AI-ENRICHED DELIVERY EVENTS (AI_GENERATE)
-- =========================================
-- Goal:
--   Use AI_GENERATE to turn messy free-text driver_notes into a structured ROW:
--     • primary_issue            (Weather, Traffic, Customer, Mechanical, Access, None, etc.)
--     • issue_category          (Delay, Damage, Both, None, etc.)
--     • estimated_delay_minutes (integer estimate from the text)
--     • requires_apology        (should customer success follow up?)
--
-- Pattern:
--   • We use AI_GENERATE with WITH SCHEMA ROW(...) to force structured JSON-like output.
--   • Here we pass ONLY text as prompt input.
--
-- How this generalizes to unstructured files:
--   • If your notes came from PDFs stored in object storage, you’d use
--       FROM TABLE(LIST_FILES('@logistics_docs/delivery_notes'))
--     and call:
--       AI_GENERATE('gpt.4o', ('Extract these fields from the document', file) WITH SCHEMA ROW(...))
--   • The ROW(prompt, file) tells Dremio to send both instruction + file content to the LLM.
-- =========================================
CREATE OR REPLACE VIEW dremio.logistics.silver.delivery_ai_enriched AS
SELECT
  de.*,

  -- AI_GENERATE: extract structured fields from free-text driver notes
  AI_GENERATE(
    'You are analyzing logistics delivery notes. '
    || 'From the following text, extract: '
    || 'primary_issue (Traffic, Weather, Access, Mechanical, Customer, None, Other), '
    || 'issue_category (Delay, Damage, Delay and Damage, None), '
    || 'estimated_delay_minutes (integer, 0 if no delay), '
    || 'requires_apology (true if customer is upset, shipment damaged, or delay > 30 min). '
    || 'Return only the structured fields.'
    || ' NOTES: ' || de.driver_notes
    WITH SCHEMA ROW(
      primary_issue            VARCHAR,
      issue_category           VARCHAR,
      estimated_delay_minutes  INT,
      requires_apology         BOOLEAN
    )
  ) AS ai_struct
FROM dremio.logistics.raw.delivery_events de;


-- Flatten the AI-generated ROW into explicit columns for easier querying.
CREATE OR REPLACE VIEW dremio.logistics.silver.delivery_ai_flat AS
SELECT
  delivery_id,
  route_id,
  driver_id,
  scheduled_dt,
  actual_dt,
  destination_city,
  destination_region,
  driver_notes,

  ai_struct['primary_issue']           AS primary_issue,
  ai_struct['issue_category']          AS issue_category,
  ai_struct['estimated_delay_minutes'] AS estimated_delay_minutes,
  ai_struct['requires_apology']        AS requires_apology
FROM dremio.logistics.silver.delivery_ai_enriched;


-- =========================================
-- 4. SILVER: RISK CLASSIFICATION (AI_CLASSIFY)
-- =========================================
-- Goal:
--   Assign a discrete risk_level label to each delivery:
--     ['Low', 'Medium', 'High', 'Critical']
--
-- Pattern:
--   • AI_CLASSIFY(prompt, text, categories_array)
--   • Categories are explicit; the LLM must pick one.
--
-- How this generalizes to unstructured files:
--   • With LIST_FILES you could classify scans of incident reports:
--       AI_CLASSIFY(
--         'gpt.4o',
--         ('Classify risk of this incident report', file),
--         ARRAY['Low','Medium','High','Critical']
--       )
--   • Useful for bulk document triage (insurance claims, accident photos, etc.).
-- =========================================
CREATE OR REPLACE VIEW dremio.logistics.silver.delivery_with_risk AS
SELECT
  daf.*,

  AI_CLASSIFY(
    'Classify the operational risk level of this delivery using the driver notes and fields. '
    || 'Consider delays, damage, customer sentiment, and recurring issues. '
    || 'Return only: Low, Medium, High, or Critical. '
    || 'Primary issue: ' || COALESCE(primary_issue, 'Unknown')
    || '. Issue category: ' || COALESCE(issue_category, 'Unknown')
    || '. Estimated delay minutes: ' || COALESCE(CAST(estimated_delay_minutes AS VARCHAR), '0')
    || '. Requires apology: ' || COALESCE(CAST(requires_apology AS VARCHAR), 'false')
    || '. Original notes: ' || driver_notes,
    ARRAY['Low', 'Medium', 'High', 'Critical']
  ) AS risk_level
FROM dremio.logistics.silver.delivery_ai_flat daf;


-- =========================================
-- 5. SILVER: AUTO-GENERATED SUMMARIES (AI_COMPLETE)
-- =========================================
-- Goal:
--   Generate a concise, operations-friendly summary for each delivery,
--   suitable for:
--     • CS ticket description
--     • Shift hand-off notes
--     • Daily summary emails
--
-- Pattern:
--   • AI_COMPLETE is a simpler, text-only interface than AI_GENERATE, always VARCHAR.
--   • Great for summaries and short narratives.
--
-- How this generalizes to unstructured files:
--   • Combine LIST_FILES + AI_GENERATE to first extract structured fields (location, time, parties).
--   • Then use AI_COMPLETE to generate a narrative summary using those fields.
-- =========================================
CREATE OR REPLACE VIEW dremio.logistics.silver.delivery_with_summary AS
SELECT
  dwr.*,
  AI_COMPLETE(
    'Write a one-sentence summary of this delivery event for logistics managers. '
    || 'Include route, city, delay vs schedule, main issue, and whether follow-up is needed. '
    || 'Be factual and concise. '
    || 'Data: '
    || 'Route: ' || route_id
    || ', City: ' || destination_city
    || ', Region: ' || destination_region
    || ', Scheduled: ' || CAST(scheduled_dt AS VARCHAR)
    || ', Actual: ' || CAST(actual_dt AS VARCHAR)
    || ', Primary issue: ' || COALESCE(primary_issue, 'Unknown')
    || ', Issue category: ' || COALESCE(issue_category, 'Unknown')
    || ', Estimated delay minutes: ' || COALESCE(CAST(estimated_delay_minutes AS VARCHAR), '0')
    || ', Requires apology: ' || COALESCE(CAST(requires_apology AS VARCHAR), 'false')
    || ', Risk level: ' || COALESCE(risk_level, 'Unknown')
    || ', Notes: ' || driver_notes
  ) AS ai_summary
FROM dremio.logistics.silver.delivery_with_risk dwr;


-- =========================================
-- 6. GOLD: DAILY DELIVERY QUALITY METRICS
-- =========================================
-- Goal:
--   Turn AI outputs into concrete metrics:
--     • On-time vs late counts
--     • Average delay minutes
--     • Distribution of risk levels
--     • Count of deliveries needing apology / CS follow-up
--
-- Pattern:
--   • AI runs in the silver layer.
--   • Gold layer is “plain SQL” over AI-enriched fields.
--   • This keeps AI cost controlled and metrics easy to consume.
-- =========================================
CREATE OR REPLACE VIEW dremio.logistics.gold.daily_delivery_quality AS
SELECT
  CAST(scheduled_dt AS DATE) AS delivery_date,
  destination_region,
  destination_city,

  COUNT(*) AS total_deliveries,

  -- On-time vs late based on actual vs scheduled
  SUM(CASE WHEN actual_dt <= scheduled_dt THEN 1 ELSE 0 END) AS on_time_deliveries,
  SUM(CASE WHEN actual_dt  > scheduled_dt THEN 1 ELSE 0 END) AS late_deliveries,

  -- AI-derived delay estimate and apology need
  AVG(COALESCE(estimated_delay_minutes, 0)) AS avg_ai_estimated_delay_minutes,
  SUM(CASE WHEN requires_apology THEN 1 ELSE 0 END) AS deliveries_requiring_apology,

  -- Risk distribution
  SUM(CASE WHEN risk_level = 'Low'      THEN 1 ELSE 0 END) AS risk_low_count,
  SUM(CASE WHEN risk_level = 'Medium'   THEN 1 ELSE 0 END) AS risk_medium_count,
  SUM(CASE WHEN risk_level = 'High'     THEN 1 ELSE 0 END) AS risk_high_count,
  SUM(CASE WHEN risk_level = 'Critical' THEN 1 ELSE 0 END) AS risk_critical_count
FROM dremio.logistics.silver.delivery_with_summary
GROUP BY
  CAST(scheduled_dt AS DATE),
  destination_region,
  destination_city;


-- =========================================
-- 7. GOLD: RISK DASHBOARD BY ROUTE
-- =========================================
-- Goal:
--   Identify high-risk routes that may need redesign, driver coaching,
--   or capacity changes.
-- =========================================
CREATE OR REPLACE VIEW dremio.logistics.gold.route_risk_dashboard AS
SELECT
  route_id,
  destination_region,
  COUNT(*) AS total_deliveries,
  AVG(COALESCE(estimated_delay_minutes, 0)) AS avg_ai_estimated_delay_minutes,
  SUM(CASE WHEN risk_level IN ('High','Critical') THEN 1 ELSE 0 END) AS high_risk_deliveries,
  SUM(CASE WHEN issue_category IN ('Delay','Delay and Damage') THEN 1 ELSE 0 END) AS delay_related_deliveries,
  SUM(CASE WHEN issue_category IN ('Damage','Delay and Damage') THEN 1 ELSE 0 END) AS damage_related_deliveries
FROM dremio.logistics.silver.delivery_with_summary
GROUP BY
  route_id,
  destination_region;


-- =========================================
-- 8. LIST_FILES PATTERN (ILLUSTRATIVE ONLY)
-- =========================================
-- This section is an EXAMPLE ONLY.
-- It shows how the same AI patterns used above would work once you
-- connect a cloud/object storage source and want to process documents
-- directly instead of VARCHAR columns.
--
-- Assume you add a source called "logistics_docs" that contains:
--   • PDF incident reports
--   • Uploaded driver photos / scans
--
-- LIST_FILES returns a table with one row per file. In Dremio’s AI functions:
--   • "file" is a special struct that includes metadata and file content handle.
--   • You pass ROW('prompt', file) into AI_GENERATE / AI_CLASSIFY.
--
-- NOTE: This query will NOT run in this environment until you configure
--       a real source named @logistics_docs/...
-- =========================================

/*
-- Example: Extract structured incident data from PDF files in object storage
WITH incident_ai AS (
  SELECT
    file['path'] AS incident_path,
    AI_GENERATE(
      'gpt.4o',
      ('From this logistics incident report, extract: '
       || 'incident_type (Delay, Damage, Loss, Safety, Other), '
       || 'primary_cause (Weather, Traffic, Access, Mechanical, Human, Other), '
       || 'estimated_cost_usd (numeric), '
       || 'customer_impact_summary (short text).',
       file
      )
      WITH SCHEMA ROW(
        incident_type          VARCHAR,
        primary_cause          VARCHAR,
        estimated_cost_usd     DOUBLE,
        customer_impact_summary VARCHAR
      )
    ) AS incident_struct
  FROM TABLE(LIST_FILES('@logistics_docs/incidents/2025/'))
)
SELECT
  incident_path,
  incident_struct['incident_type']          AS incident_type,
  incident_struct['primary_cause']         AS primary_cause,
  incident_struct['estimated_cost_usd']    AS estimated_cost_usd,
  incident_struct['customer_impact_summary'] AS customer_impact_summary
FROM incident_ai;
*/

-- How this relates back to our table-based example:
--   • In this script we stored "documents" as VARCHAR (driver_notes) in a table.
--   • With LIST_FILES, the "documents" live as files in object storage.
--   • The AI functions and schemas are essentially the same:
--       - AI_GENERATE for multi-field extraction
--       - AI_CLASSIFY for discrete labels on each file
--       - AI_COMPLETE for summaries or narratives
--   • You can then JOIN the extracted data back to operational tables
--     using keys extracted from the document (delivery_id, route, date).


-- ====================================================================================
-- END OF LOGISTICS AI EXAMPLE
-- ====================================================================================
