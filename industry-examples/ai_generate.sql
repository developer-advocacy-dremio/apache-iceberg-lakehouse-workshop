-- Create Folder
CREATE FOLDER IF NOT EXISTS dremio.ai_generate_example;

-- CREATE TABLE FROM PDFS
CREATE TABLE IF NOT EXISTS dremio.ai_generate_example.people AS
SELECT
    file['path'] as file_path,
    AI_GENERATE(
        (
            'Extract the name, age and birthdate on these files',
            file
        )
        WITH SCHEMA ROW(
            first_last_name VARCHAR,
            age INTEGER,
            birth_date VARCHAR
        )
    ) AS extracted
FROM TABLE(
    LIST_FILES('@s3/alex-merced-demo-2026/pdf-example/')
)
WHERE file['path'] LIKE '%.pdf';