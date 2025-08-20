-- Base folders (namespaces)
CREATE FOLDER IF NOT EXISTS dremio.supply_chain;
CREATE FOLDER IF NOT EXISTS dremio.supply_chain.raw;
CREATE FOLDER IF NOT EXISTS dremio.supply_chain.silver;
CREATE FOLDER IF NOT EXISTS dremio.supply_chain.gold;

-- --------------------------------------------------------
-- BRONZE: Raw purchase orders (one row per PO line)
-- --------------------------------------------------------
CREATE TABLE IF NOT EXISTS dremio.supply_chain.raw.purchase_orders (
  po_id           BIGINT,
  line_id         BIGINT,
  supplier_id     VARCHAR,
  sku             VARCHAR,
  order_qty       BIGINT,
  unit_price      DECIMAL(18,4),
  currency        VARCHAR,
  order_ts        TIMESTAMP,          -- when the PO line was created
  due_date        DATE,               -- expected arrival date (per PO line)
  ship_to_wh      VARCHAR             -- warehouse/location code
)
PARTITION BY (DAY(order_ts));

-- Sample data
INSERT INTO dremio.supply_chain.raw.purchase_orders
(po_id, line_id, supplier_id, sku, order_qty, unit_price, currency, order_ts, due_date, ship_to_wh) VALUES
  (5001, 1, 'SUP-ACME', 'SKU-APPLE-001', 100, 0.45, 'USD', TIMESTAMP '2025-08-18 08:05:00', DATE '2025-08-19', 'WH-01'),
  (5001, 2, 'SUP-ACME', 'SKU-PEAR-002' ,  60, 0.30, 'USD', TIMESTAMP '2025-08-18 08:05:00', DATE '2025-08-19', 'WH-01'),
  (5002, 1, 'SUP-BRAVO','SKU-PEACH-010',  80, 0.55, 'USD', TIMESTAMP '2025-08-18 09:10:00', DATE '2025-08-20', 'WH-02'),
  (5003, 1, 'SUP-ACME', 'SKU-APPLE-001',  50, 0.46, 'USD', TIMESTAMP '2025-08-19 10:00:00', DATE '2025-08-20', 'WH-01'),
  (5004, 1, 'SUP-DELTA','SKU-BANANA-005',120, 0.25, 'USD', TIMESTAMP '2025-08-19 10:30:00', DATE '2025-08-21', 'WH-03'),
  (5005, 1, 'SUP-BRAVO','SKU-PEACH-010',  40, 0.56, 'USD', TIMESTAMP '2025-08-20 11:15:00', DATE '2025-08-21', 'WH-02');

-- --------------------------------------------------------
-- BRONZE: Raw receipts (warehouse receipts against PO lines)
-- (For simplicity, assume one receipt per PO line in the sample.)
-- --------------------------------------------------------
CREATE TABLE IF NOT EXISTS dremio.supply_chain.raw.receipts (
  receipt_id      BIGINT,
  po_id           BIGINT,
  line_id         BIGINT,
  sku             VARCHAR,
  received_qty    BIGINT,
  receipt_ts      TIMESTAMP,          -- actual receipt timestamp
  warehouse_id    VARCHAR
)
PARTITION BY (DAY(receipt_ts));

-- Sample data
INSERT INTO dremio.supply_chain.raw.receipts
(receipt_id, po_id, line_id, sku, received_qty, receipt_ts, warehouse_id) VALUES
  (9001, 5001, 1, 'SKU-APPLE-001', 100, TIMESTAMP '2025-08-19 07:35:00', 'WH-01'), -- on time, in full
  (9002, 5001, 2, 'SKU-PEAR-002' ,  55, TIMESTAMP '2025-08-19 12:10:00', 'WH-01'), -- on time, short 5
  (9003, 5002, 1, 'SKU-PEACH-010',  80, TIMESTAMP '2025-08-20 09:00:00', 'WH-02'), -- on time, in full
  (9004, 5003, 1, 'SKU-APPLE-001',  50, TIMESTAMP '2025-08-20 15:20:00', 'WH-01'), -- on time, in full
  (9005, 5004, 1, 'SKU-BANANA-005',110, TIMESTAMP '2025-08-22 08:00:00', 'WH-03'), -- late (due 8/21), short 10
  (9006, 5005, 1, 'SKU-PEACH-010',  40, TIMESTAMP '2025-08-21 10:45:00', 'WH-02'); -- on time, in full

-- --------------------------------------------------------
-- SILVER: Clean join of PO lines and Receipts
-- Adds OTIF signals and key dates aligned to day
-- --------------------------------------------------------
CREATE OR REPLACE VIEW dremio.supply_chain.silver.po_receipts_enriched AS
SELECT
  po.po_id,
  po.line_id,
  po.supplier_id,
  po.sku,
  po.order_qty,
  po.unit_price,
  po.currency,
  TO_DATE(po.order_ts)                   AS order_date,
  po.due_date,
  po.ship_to_wh,
  r.receipt_id,
  r.received_qty,
  TO_DATE(r.receipt_ts)                  AS receipt_date,
  r.warehouse_id,
  -- on time if receipt_date <= due_date
  CASE WHEN TO_DATE(r.receipt_ts) <= po.due_date THEN 1 ELSE 0 END AS on_time_flag,
  -- in full if received >= ordered
  CASE WHEN r.received_qty >= po.order_qty THEN 1 ELSE 0 END       AS in_full_flag,
  -- OTIF if both true
  CASE 
    WHEN TO_DATE(r.receipt_ts) <= po.due_date AND r.received_qty >= po.order_qty 
    THEN 1 ELSE 0 
  END AS otif_flag
FROM dremio.supply_chain.raw.purchase_orders po
JOIN dremio.supply_chain.raw.receipts r
  ON po.po_id = r.po_id
 AND po.line_id = r.line_id;

-- --------------------------------------------------------
-- GOLD: Business view - OTIF rate and Fill Rate by Supplier and Day
-- Answers: "What are daily delivery performance KPIs per supplier?"
-- --------------------------------------------------------
CREATE OR REPLACE VIEW dremio.supply_chain.gold.daily_supplier_delivery_kpis AS
SELECT
  supplier_id,
  receipt_date,
  COUNT(*)                                                        AS lines_received,
  AVG(otif_flag)                                                  AS otif_rate,      -- share of lines both on time and in full
  AVG(on_time_flag)                                               AS on_time_rate,   -- share of lines on time
  AVG(in_full_flag)                                               AS in_full_rate,   -- share of lines in full
  SUM(received_qty)                                               AS total_received_qty,
  SUM(order_qty)                                                  AS total_order_qty,
  CASE WHEN SUM(order_qty) > 0 
       THEN SUM(received_qty) * 1.0 / SUM(order_qty) 
       ELSE NULL 
  END                                                             AS gross_fill_rate -- quantity-based fill rate
FROM dremio.supply_chain.silver.po_receipts_enriched
GROUP BY supplier_id, receipt_date;

-- (Optional) GOLD: Inventory cost impact (simple landed cost proxy)
-- If helpful, you can value receipts at PO unit price for a daily cost-of-receipts view.
CREATE OR REPLACE VIEW dremio.supply_chain.gold.daily_receipt_costs AS
SELECT
  supplier_id,
  sku,
  receipt_date,
  SUM(received_qty * unit_price) AS total_receipt_cost
FROM dremio.supply_chain.silver.po_receipts_enriched
GROUP BY supplier_id, sku, receipt_date;