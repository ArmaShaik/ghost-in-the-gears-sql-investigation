-- ===============================
-- "The Ghost in the Gears" – SQL Murder Mystery Investigation
-- Author: Arma Shaik
-- ===============================

-- ===============================
-- 1. Environment Setup
-- ===============================

USE ROLE sysadmin;

CREATE OR REPLACE WAREHOUSE ghosts_loading_wh 
  WITH WAREHOUSE_SIZE = 'X-SMALL' AUTO_RESUME = TRUE AUTO_SUSPEND = 180 INITIALLY_SUSPENDED = TRUE;

CREATE OR REPLACE WAREHOUSE ghosts_query_wh 
  WITH WAREHOUSE_SIZE = 'X-SMALL' AUTO_RESUME = TRUE AUTO_SUSPEND = 180 INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS investigation;
USE DATABASE investigation;
USE SCHEMA public;

USE ROLE accountadmin;

CREATE OR REPLACE RESOURCE MONITOR ghosts_rm
  WITH CREDIT_QUOTA = 200
  FREQUENCY = monthly
  START_TIMESTAMP = IMMEDIATELY
  TRIGGERS 
    ON 80 PERCENT DO NOTIFY,
    ON 100 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE ghosts_query_wh SET RESOURCE_MONITOR = ghosts_rm;
ALTER WAREHOUSE ghosts_query_wh SET STATEMENT_TIMEOUT_IN_SECONDS = 3600;
ALTER WAREHOUSE ghosts_query_wh SET STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = 900;

-- ===============================
-- 2. Table Creation
-- ===============================

USE ROLE sysadmin;
USE WAREHOUSE ghosts_loading_wh;

CREATE OR REPLACE TABLE crime_details (
    crime_id INT, victim_id INT, date DATE, time TIME,
    location STRING, type STRING
);

CREATE OR REPLACE TABLE victim_profiles (
    victim_id INT, name STRING, age INT, occupation STRING,
    address STRING, district STRING
);

CREATE OR REPLACE TABLE forum_activities (
    post_id INT, user_id INT, user_ip_address STRING,
    post_title STRING, post_category STRING, post_date DATE
);

CREATE OR REPLACE TABLE city_officials (
    official_id INT, name STRING, position STRING, department STRING,
    office_location STRING, tenure_start DATE, ip_address STRING, phone_directory_id INT
);

CREATE OR REPLACE TABLE phone_directory (
    directory_id INT, phone_number STRING, name STRING,
    address STRING, district STRING
);

CREATE OR REPLACE TABLE call_log (
    call_id INT, caller_id INT, receiver_id INT,
    call_duration INT, call_start_date DATE, call_status STRING
);

CREATE OR REPLACE TABLE video_activity_json (v VARIANT);

-- ===============================
-- 3. Data Loading (S3 → Snowflake)
-- ===============================

CREATE OR REPLACE FILE FORMAT ghosts_csv
  TYPE = 'CSV' SKIP_HEADER = 1 TRIM_SPACE = TRUE NULL_IF = ('-');

CREATE OR REPLACE FILE FORMAT ghosts_json
  TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE;

CREATE OR REPLACE STAGE investigation1026 URL = 's3://investigation-1026/';
COPY INTO crime_details FROM @investigation1026/crime_details FILE_FORMAT = ghosts_csv PATTERN = '.*csv.*';
COPY INTO victim_profiles FROM @investigation1026/victim_profiles FILE_FORMAT = ghosts_csv PATTERN = '.*csv.*';

CREATE OR REPLACE STAGE investigation2134 URL = 's3://investigation-2134/';
COPY INTO forum_activities FROM @investigation2134/forum_activities FILE_FORMAT = ghosts_csv PATTERN = '.*csv.*';
COPY INTO city_officials FROM @investigation2134/city_officials FILE_FORMAT = ghosts_csv PATTERN = '.*csv.*';

CREATE OR REPLACE STAGE investigation3651 URL = 's3://investigation-3651/';
COPY INTO call_log FROM @investigation3651/call_log FILE_FORMAT = ghosts_csv PATTERN = '.*csv.*';
COPY INTO phone_directory FROM @investigation3651/phone_directory FILE_FORMAT = ghosts_csv PATTERN = '.*csv.*';

CREATE OR REPLACE STAGE investigation9134 URL = 's3://investigation-9134/';
COPY INTO video_activity_json FROM @investigation9134/video_activities FILE_FORMAT = ghosts_json PATTERN = '.*json.*';

CREATE OR REPLACE STAGE investigation8547 URL = 's3://investigation-8547/';
COPY INTO city_officials FROM @investigation8547/enriched_city_officials FILE_FORMAT = ghosts_csv PATTERN = '.*csv.*';

-- ===============================
-- 4. View Creation
-- ===============================

CREATE OR REPLACE VIEW victim_detail_view AS
WITH homicide_victims AS (
    SELECT vp.victim_id, vp.name, vp.address, cd.date AS homicide_date, pd.directory_id
    FROM victim_profiles vp
    JOIN crime_details cd ON vp.victim_id = cd.victim_id
    LEFT JOIN phone_directory pd ON vp.address = pd.address
    WHERE cd.type = 'Homicide'
)
SELECT * FROM homicide_victims;

CREATE OR REPLACE VIEW forum_posts_by_officials AS
SELECT co.name, fa.post_title, co.department, fa.post_category, fa.post_date
FROM city_officials co
JOIN forum_activities fa ON co.ip_address = fa.user_ip_address
WHERE LOWER(fa.post_title) LIKE '%ai%' OR LOWER(fa.post_category) LIKE '%ai%';

CREATE OR REPLACE VIEW calls_to_victims AS
SELECT v.name AS victim_name, c.call_id, c.call_start_date, c.call_status, v.homicide_date, c.caller_id, p.name AS caller_name
FROM victim_detail_view v
LEFT JOIN call_log c ON v.directory_id = c.receiver_id
JOIN phone_directory p ON c.caller_id = p.directory_id
WHERE DATEDIFF(DAY, c.call_start_date, v.homicide_date) BETWEEN 0 AND 14
  AND c.call_start_date <= v.homicide_date;

CREATE OR REPLACE VIEW calls_to_victims_by_officials AS
SELECT v.call_id, v.victim_name, v.call_start_date, co.name, co.phone_directory_id
FROM calls_to_victims v
JOIN city_officials co ON v.caller_id = co.phone_directory_id;

-- ===============================
-- 5. Analysis & Data Exploration
-- ===============================

-- Crime stats
SELECT COUNT(*) AS total_crimes FROM crime_details;

SELECT type, COUNT(*) FROM crime_details GROUP BY type;

SELECT COUNT(*) AS night_crimes
FROM crime_details
WHERE time >= '21:00:00' OR time < '06:00:00';

SELECT ROUND(AVG(age), 2) AS avg_age
FROM victim_profiles
WHERE victim_id IN (
  SELECT victim_id FROM crime_details WHERE type = 'Homicide'
);

-- Call log analysis
SELECT COUNT(*) AS total_calls,
  SUM(CASE WHEN call_status = 'answered' THEN 1 ELSE 0 END) AS answered_calls,
  ROUND(SUM(CASE WHEN call_status = 'answered' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 0) AS pct_answered
FROM call_log;

SELECT ROUND(AVG(call_duration) / 60.0, 1) AS avg_duration_mins
FROM call_log
WHERE call_status = 'answered';

SELECT EXTRACT(YEAR FROM call_start_date) AS year,
       EXTRACT(MONTH FROM call_start_date) AS month,
       COUNT(call_id) AS monthly_total,
       SUM(COUNT(call_id)) OVER(ORDER BY EXTRACT(YEAR FROM call_start_date), EXTRACT(MONTH FROM call_start_date)) AS cumulative_total
FROM call_log
GROUP BY year, month;

SELECT pd.district, COUNT(*) AS outbound_call_volume
FROM call_log c
JOIN phone_directory pd ON c.caller_id = pd.directory_id
GROUP BY pd.district
ORDER BY outbound_call_volume DESC;

-- Video analysis
SELECT MIN(timestamp) AS earliest, MAX(timestamp) AS latest
FROM video_activity_view;

SELECT DISTINCT v.*
FROM video_activity_view v
JOIN victim_detail_view d
  ON TO_DATE(v.timestamp) = d.homicide_date
ORDER BY v.timestamp DESC;

SELECT *
FROM video_activity_view
WHERE TO_DATE(timestamp) = '2023-12-28'
ORDER BY timestamp DESC;

-- ===============================
-- 6. Final Suspect Identification
-- ===============================

SELECT DISTINCT f.name AS suspect_name
FROM forum_posts_by_officials f
JOIN calls_to_victims_by_officials c
  ON f.name = c.name
ORDER BY suspect_name;

-- ===============================
-- 7. Access Control & Permissions
-- ===============================

USE ROLE useradmin;
CREATE OR REPLACE ROLE ghosts_query_role;
GRANT ROLE ghosts_query_role TO USER YOUR_USERNAME;

USE ROLE securityadmin;
GRANT OPERATE, USAGE ON WAREHOUSE ghosts_query_wh TO ROLE ghosts_query_role;
GRANT USAGE ON DATABASE investigation TO ROLE ghosts_query_role;
GRANT USAGE ON SCHEMA investigation.public TO ROLE ghosts_query_role;
GRANT SELECT ON ALL TABLES IN SCHEMA investigation.public TO ROLE ghosts_query_role;
GRANT SELECT ON ALL VIEWS IN SCHEMA investigation.public TO ROLE ghosts_query_role;
