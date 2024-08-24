USE DATABASE quickstart;
USE SCHEMA ml_functions;

-- Note: It's important to update the recipient email twice in the code below
-- Create a task to run every month to retrain the anomaly detection model: 
CREATE OR REPLACE TASK ad_vancouver_training_task
    WAREHOUSE = quickstart_wh
    SCHEDULE = 'USING CRON 0 0 1 * * Asia/Singapore' -- Runs once a month
AS
CREATE OR REPLACE snowflake.ml.anomaly_detection vancouver_anomaly_model(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'vancouver_anomaly_training_set'),
    SERIES_COLNAME => 'MENU_ITEM_NAME',
    TIMESTAMP_COLNAME => 'TIMESTAMP',
    TARGET_COLNAME => 'TOTAL_SOLD',
    LABEL_COLNAME => ''
);

-- Note: It's important to update the recipient email twice in the code below
-- Create a task to run every month to retrain the anomaly detection model: 
CREATE OR REPLACE TASK ad_vancouver_training_task
    WAREHOUSE = quickstart_wh
    SCHEDULE = 'USING CRON 0 0 1 * * America/Los_Angeles' -- Runs once a month
AS
CREATE OR REPLACE snowflake.ml.anomaly_detection vancouver_anomaly_model(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'vancouver_anomaly_training_set'),
    SERIES_COLNAME => 'MENU_ITEM_NAME',
    TIMESTAMP_COLNAME => 'TIMESTAMP',
    TARGET_COLNAME => 'TOTAL_SOLD',
    LABEL_COLNAME => ''
);

-- Creates a Stored Procedure to extract the anomalies from our freshly trained model: 
CREATE OR REPLACE PROCEDURE extract_anomalies()
RETURNS TABLE ()
LANGUAGE sql 
AS
BEGIN
    CALL vancouver_anomaly_model!DETECT_ANOMALIES(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'vancouver_anomaly_analysis_set'),
    SERIES_COLNAME => 'MENU_ITEM_NAME',
    TIMESTAMP_COLNAME => 'TIMESTAMP',
    TARGET_COLNAME => 'TOTAL_SOLD',
    CONFIG_OBJECT => {'prediction_interval': 0.95});
DECLARE res RESULTSET DEFAULT (
    SELECT series, is_anomaly, count(is_anomaly) as num_records 
    FROM TABLE(result_scan(-1)) 
    WHERE is_anomaly = 1 
    GROUP BY ALL
    HAVING num_records > 5
    ORDER BY num_records DESC);
BEGIN 
    RETURN table(res);
END;
END;

-- Create an email integration: 
CREATE OR REPLACE NOTIFICATION INTEGRATION my_email_int
TYPE = EMAIL
ENABLED = TRUE
ALLOWED_RECIPIENTS = ('kamesh.sampath@hotmail.com');  -- update the recipient's email here


-- Create Snowpark Python Stored Procedure to format email and send it
CREATE OR REPLACE PROCEDURE send_anomaly_report()
RETURNS string
LANGUAGE python
runtime_version = 3.9
packages = ('snowflake-snowpark-python')
handler = 'send_email'
-- update the recipient's email below
AS
$$
def send_email(session):
    session.call('extract_anomalies').collect()
    printed = session.sql(
        "select * from table(result_scan(last_query_id(-1)))"
      ).to_pandas().to_html()
    session.call('system$send_email',
        'my_email_int',
        'kamesh.sampath@hotmail.com',
        'Email Alert: Anomaly Report Has Been created',
        printed,
        'text/html')
$$;

-- Orchestrating the Tasks: 
CREATE OR REPLACE TASK send_anomaly_report_task
    warehouse = quickstart_wh
    AFTER AD_VANCOUVER_TRAINING_TASK
    AS CALL send_anomaly_report();

-- Steps to resume and then immediately execute the task DAG:  
ALTER TASK SEND_ANOMALY_REPORT_TASK RESUME;
ALTER TASK AD_VANCOUVER_TRAINING_TASK RESUME;
EXECUTE TASK AD_VANCOUVER_TRAINING_TASK;

