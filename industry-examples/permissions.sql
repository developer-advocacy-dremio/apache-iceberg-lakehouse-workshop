-- Give Permissions to project
GRANT SELECT, VIEW REFLECTION, VIEW JOB HISTORY, USAGE, MONITOR,
       CREATE TABLE, INSERT, UPDATE, DELETE, DROP, ALTER, EXTERNAL QUERY, ALTER REFLECTION, OPERATE
ON PROJECT
TO USER "alphatest2user@alexmerced.com";

-- Give Permissions to Namespace in Catalog
GRANT USAGE on FOLDER "dremio" to USER "alphatest2user@alexmerced.com";
