-- =========================================
-- Namespaces
-- =========================================
CREATE FOLDER IF NOT EXISTS dremio.finance;
CREATE FOLDER IF NOT EXISTS dremio.finance.raw;
CREATE FOLDER IF NOT EXISTS dremio.finance.silver;
CREATE FOLDER IF NOT EXISTS dremio.finance.gold;

-- =========================================
-- BRONZE: Raw trade events (as received)
-- - Minimal transformation
-- - Keep original timestamps, currencies, sides
-- - Partition on day for pruning
-- =========================================
CREATE TABLE IF NOT EXISTS dremio.finance.raw.trades (
  trade_id     BIGINT,
  account_id   VARCHAR,
  symbol       VARCHAR,
  side         VARCHAR,          -- BUY / SELL
  qty          BIGINT,
  exec_price   DECIMAL(18,4),
  currency     VARCHAR,
  trade_ts     TIMESTAMP
)
PARTITION BY (DAY(trade_ts));

-- Sample data
INSERT INTO dremio.finance.raw.trades (trade_id, account_id, symbol, side, qty, exec_price, currency, trade_ts) VALUES
  (1001, 'ACC-001', 'AAPL', 'BUY' ,  50, 193.20, 'USD', TIMESTAMP '2025-08-18 14:05:12'),
  (1002, 'ACC-002', 'AAPL', 'SELL',  20, 194.10, 'USD', TIMESTAMP '2025-08-18 15:22:45'),
  (1003, 'ACC-001', 'MSFT', 'BUY' ,  30, 423.55, 'USD', TIMESTAMP '2025-08-18 16:01:03'),
  (1004, 'ACC-003', 'GOOG', 'BUY' ,  10, 170.80, 'USD', TIMESTAMP '2025-08-19 10:10:10'),
  (1005, 'ACC-002', 'AAPL', 'BUY' ,  15, 195.05, 'USD', TIMESTAMP '2025-08-19 11:45:00'),
  (1006, 'ACC-001', 'MSFT', 'SELL', 10, 424.25, 'USD', TIMESTAMP '2025-08-19 13:30:30'),
  (1007, 'ACC-003', 'GOOG', 'SELL',  5, 171.40, 'USD', TIMESTAMP '2025-08-20 09:35:20'),
  (1008, 'ACC-004', 'AMZN', 'BUY' ,  12, 183.10, 'USD', TIMESTAMP '2025-08-20 09:50:00');

-- =========================================
-- BRONZE: Raw end-of-day prices
-- - One row per symbol per trading date
-- - Currency kept for parity with trades
-- - Partition on price_date
-- =========================================
CREATE TABLE IF NOT EXISTS dremio.finance.raw.daily_prices (
  symbol       VARCHAR,
  price_date   DATE,
  close_price  DECIMAL(18,4),
  currency     VARCHAR
)
PARTITION BY (price_date);

-- Sample data
INSERT INTO dremio.finance.raw.daily_prices (symbol, price_date, close_price, currency) VALUES
  ('AAPL', DATE '2025-08-18', 194.25, 'USD'),
  ('MSFT', DATE '2025-08-18', 424.10, 'USD'),
  ('GOOG', DATE '2025-08-18', 170.10, 'USD'),
  ('AMZN', DATE '2025-08-18', 182.50, 'USD'),

  ('AAPL', DATE '2025-08-19', 195.50, 'USD'),
  ('MSFT', DATE '2025-08-19', 423.90, 'USD'),
  ('GOOG', DATE '2025-08-19', 171.05, 'USD'),
  ('AMZN', DATE '2025-08-19', 183.00, 'USD'),

  ('AAPL', DATE '2025-08-20', 196.20, 'USD'),
  ('MSFT', DATE '2025-08-20', 425.00, 'USD'),
  ('GOOG', DATE '2025-08-20', 171.60, 'USD'),
  ('AMZN', DATE '2025-08-20', 183.40, 'USD');

-- =========================================
-- SILVER: Enriched trades (cleansed + joined)
-- - Join trades to the matching EOD close price by symbol/date
-- - Provide trade_date and execution-vs-close delta
-- =========================================
CREATE OR REPLACE VIEW dremio.finance.silver.trades_enriched AS
SELECT
  t.trade_id,
  t.account_id,
  t.symbol,
  t.side,
  t.qty,
  t.exec_price,
  t.currency,
  TO_DATE(t.trade_ts)              AS trade_date,
  p.close_price                    AS eod_close_price,
  t.exec_price - p.close_price     AS exec_vs_close_diff
FROM dremio.finance.raw.trades t
JOIN dremio.finance.raw.daily_prices p
  ON t.symbol = p.symbol
 AND TO_DATE(t.trade_ts) = p.price_date;

-- =========================================
-- GOLD: Business metric – daily P&L by account & symbol
-- - BUY: (close - exec) * qty
-- - SELL: (exec - close) * qty
-- - Aggregated at (account_id, symbol, trade_date)
-- =========================================
CREATE OR REPLACE VIEW dremio.finance.gold.pnl_by_account_symbol AS
SELECT
  account_id,
  symbol,
  trade_date,
  SUM(
    CASE 
      WHEN side = 'BUY'  THEN (eod_close_price - exec_price) * qty
      WHEN side = 'SELL' THEN (exec_price - eod_close_price) * qty
    END
  ) AS pnl
FROM dremio.finance.silver.trades_enriched
GROUP BY account_id, symbol, trade_date;