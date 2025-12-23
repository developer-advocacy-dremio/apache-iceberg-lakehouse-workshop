/****************************************************************************************
 STEP 1
 Create the top-level namespace / folder called `example`

 - IF NOT EXISTS makes this idempotent
 - Safe to run multiple times
****************************************************************************************/
CREATE FOLDER IF NOT EXISTS example;


/****************************************************************************************
 STEP 2
 Create the medallion-style subfolders inside `example`

 Folder structure:
   example/
     ├── raw
     ├── silver
     └── gold
****************************************************************************************/
CREATE FOLDER IF NOT EXISTS example.raw;
CREATE FOLDER IF NOT EXISTS example.silver;
CREATE FOLDER IF NOT EXISTS example.gold;


/****************************************************************************************
 STEP 3
 Create a RAW table that will act as the ingestion layer

 Design goals of this table:
 - Exactly 20 records
 - Intentionally includes:
     • duplicate rows
     • NULL values
     • multiple event types
 - Supports downstream metric derivation (silver / gold layers)

 This table represents "raw, untrusted data"
****************************************************************************************/
CREATE TABLE example.raw.events (
  event_id     INT,              -- unique identifier for the event (not guaranteed unique yet)
  user_id      INT,              -- user performing the event
  event_type   VARCHAR,          -- e.g. purchase, refund
  amount       DECIMAL(10,2),     -- monetary value (can be NULL in raw)
  event_ts     TIMESTAMP          -- event timestamp
);


/****************************************************************************************
 STEP 3a
 Insert 20 raw records

 Notes:
 - Rows 1 & 2 are duplicates
 - Rows 6 & 7 are duplicates
 - Rows 5 & 15 have NULL amounts
 - Refunds have negative amounts
****************************************************************************************/
INSERT INTO example.raw.events VALUES
(1, 101, 'purchase', 25.00, TIMESTAMP '2024-01-01 10:00:00'),
(2, 101, 'purchase', 25.00, TIMESTAMP '2024-01-01 10:00:00'), -- duplicate
(3, 102, 'purchase', 40.00, TIMESTAMP '2024-01-01 11:00:00'),
(4, 103, 'refund',  -10.00, TIMESTAMP '2024-01-01 12:00:00'),
(5, 104, 'purchase', NULL,  TIMESTAMP '2024-01-01 13:00:00'), -- null amount
(6, 105, 'purchase', 15.00, TIMESTAMP '2024-01-01 14:00:00'),
(7, 105, 'purchase', 15.00, TIMESTAMP '2024-01-01 14:00:00'), -- duplicate
(8, 106, 'purchase', 55.00, TIMESTAMP '2024-01-02 09:00:00'),
(9, 107, 'purchase', 30.00, TIMESTAMP '2024-01-02 10:00:00'),
(10,108, 'refund',  -5.00,  TIMESTAMP '2024-01-02 11:00:00'),
(11,109, 'purchase', 70.00, TIMESTAMP '2024-01-02 12:00:00'),
(12,110, 'purchase', 20.00, TIMESTAMP '2024-01-02 13:00:00'),
(13,111, 'purchase', 35.00, TIMESTAMP '2024-01-03 09:00:00'),
(14,112, 'purchase', 60.00, TIMESTAMP '2024-01-03 10:00:00'),
(15,113, 'purchase', NULL,  TIMESTAMP '2024-01-03 11:00:00'), -- null amount
(16,114, 'refund',  -20.00, TIMESTAMP '2024-01-03 12:00:00'),
(17,115, 'purchase', 90.00, TIMESTAMP '2024-01-03 13:00:00'),
(18,116, 'purchase', 45.00, TIMESTAMP '2024-01-04 09:00:00'),
(19,117, 'purchase', 50.00, TIMESTAMP '2024-01-04 10:00:00'),
(20,118, 'purchase', 80.00, TIMESTAMP '2024-01-04 11:00:00');


/****************************************************************************************
 END STATE

 You now have:
 - example           → top-level namespace
 - example.raw       → raw ingestion layer
 - example.silver    → ready for cleansing views
 - example.gold      → ready for metrics

 Next steps (not included here):
 - Silver view: dedupe + NULL handling
 - Gold views: revenue, counts, averages, daily metrics
****************************************************************************************/

/****************************************************************************************
 SILVER LAYER
 Create a cleansed view over the RAW events table

 Goals of this SILVER view:
 - Remove duplicate records
 - Handle NULL amounts safely
 - Establish a clean, trusted dataset for metric derivation

 Cleansing rules applied:
 1. Deduplication:
    - Duplicates are defined as rows with the same:
        (user_id, event_type, amount, event_ts)
    - We keep the earliest event_id using ROW_NUMBER()

 2. NULL handling:
    - NULL amounts are converted to 0.00
    - This prevents aggregation errors in GOLD metrics

 3. Schema stability:
    - Explicit column selection
    - No SELECT *
****************************************************************************************/

CREATE OR REPLACE VIEW example.silver.events_clean AS
SELECT
  event_id,
  user_id,
  event_type,

  -- Replace NULL monetary values with 0.00
  COALESCE(amount, 0.00) AS amount,

  event_ts
FROM (
  SELECT
    event_id,
    user_id,
    event_type,
    amount,
    event_ts,

    -- Assign a row number to detect duplicates
    ROW_NUMBER() OVER (
      PARTITION BY
        user_id,
        event_type,
        amount,
        event_ts
      ORDER BY
        event_id
    ) AS row_num
  FROM example.raw.events
)
-- Keep only the first occurrence of each duplicate group
WHERE row_num = 1;


/****************************************************************************************
 END STATE (SILVER)

 example.silver.events_clean now represents:
 - Deduplicated data
 - No NULL amounts
 - A stable, trusted layer for analytics

 This view is suitable for:
 - Aggregations
 - BI tools
 - Downstream GOLD metrics
****************************************************************************************/

/****************************************************************************************
 GOLD LAYER
 Create metric-focused views derived from the SILVER (clean) dataset

 Design principles for GOLD:
 - Business-ready metrics
 - No raw cleansing logic
 - Stable, semantic definitions
 - Each metric is explicit and reusable

 Source:
   example.silver.events_clean
****************************************************************************************/


/****************************************************************************************
 METRIC 1
 Total Revenue (net of refunds)

 Business logic:
 - Purchases contribute positive amounts
 - Refunds contribute negative amounts
****************************************************************************************/
CREATE OR REPLACE VIEW example.gold.total_revenue AS
SELECT
  SUM(amount) AS total_revenue
FROM example.silver.events_clean;


/****************************************************************************************
 METRIC 2
 Total Purchase Count

 Business logic:
 - Counts only purchase events
 - Deduplicated and NULL-safe by SILVER layer
****************************************************************************************/
CREATE OR REPLACE VIEW example.gold.purchase_count AS
SELECT
  COUNT(*) AS purchase_count
FROM example.silver.events_clean
WHERE event_type = 'purchase';


/****************************************************************************************
 METRIC 3
 Average Purchase Amount (AOV)

 Business logic:
 - Average of purchase amounts
 - NULLs already converted to 0.00 in SILVER
****************************************************************************************/
CREATE OR REPLACE VIEW example.gold.avg_purchase_amount AS
SELECT
  AVG(amount) AS avg_purchase_amount
FROM example.silver.events_clean
WHERE event_type = 'purchase';


/****************************************************************************************
 METRIC 4
 Daily Revenue

 Business logic:
 - Revenue aggregated by event date
 - Useful for trend reporting and dashboards
****************************************************************************************/
CREATE OR REPLACE VIEW example.gold.daily_revenue AS
SELECT
  CAST(event_ts AS DATE) AS event_date,
  SUM(amount) AS daily_revenue
FROM example.silver.events_clean
GROUP BY CAST(event_ts AS DATE)
ORDER BY event_date;


/****************************************************************************************
 END STATE (GOLD)

 You now have four business-ready metric views:

 - example.gold.total_revenue
 - example.gold.purchase_count
 - example.gold.avg_purchase_amount
 - example.gold.daily_revenue

 These views:
 - Are stable for BI tools
 - Can be accelerated with reflections
 - Form a clean semantic layer
****************************************************************************************/
