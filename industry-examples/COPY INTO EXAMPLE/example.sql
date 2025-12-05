-- create folder
CREATE FOLDER IF NOT EXISTS dremio.copy_demo;

-- create table
CREATE TABLE dremio.copy_demo.transactions (
    id INTEGER,
    name VARCHAR,
    amount DOUBLE,
    event_date DATE
);

-- copy files into
COPY INTO dremio.copy_demo.transactions
FROM '@s3/alex-merced-demo-2026/copy-into-example/'
REGEX '.*\.csv'
FILE_FORMAT 'csv'
(
  EXTRACT_HEADER = TRUE,
  FIELD_DELIMITER = ',',
  RECORD_DELIMITER = '\n'
);
