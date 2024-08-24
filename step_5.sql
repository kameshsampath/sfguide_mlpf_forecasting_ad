USE DATABASE quickstart;
USE SCHEMA ml_functions;

SELECT MAX(timestamp) FROM vancouver_sales;

Set ts_timestamp = (
SELECT MAX(timestamp) FROM vancouver_sales) - interval '1 Month';

select $ts_timestamp;

-- Create a view containing our training data
CREATE OR REPLACE VIEW vancouver_anomaly_training_set AS (
    SELECT *
    FROM vancouver_sales
    WHERE timestamp < (SELECT MAX(timestamp) FROM vancouver_sales) - interval '1 Month'
);

-- Create a view containing the data we want to make inferences on
CREATE OR REPLACE VIEW vancouver_anomaly_analysis_set AS (
    SELECT *
    FROM vancouver_sales
    WHERE timestamp > (SELECT MAX(timestamp) FROM vancouver_anomaly_training_set)
);

-- Create the model: UNSUPERVISED method, however can pass labels as well; this could take ~15-25 secs; please be patient 
CREATE OR REPLACE snowflake.ml.anomaly_detection vancouver_anomaly_model(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'vancouver_anomaly_training_set'),
    SERIES_COLNAME => 'MENU_ITEM_NAME',
    TIMESTAMP_COLNAME => 'TIMESTAMP',
    TARGET_COLNAME => 'TOTAL_SOLD',
    LABEL_COLNAME => ''
);

-- Call the model and store the results into table; this could take ~10-20 secs; please be patient
CALL vancouver_anomaly_model!DETECT_ANOMALIES(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'vancouver_anomaly_analysis_set'),
    SERIES_COLNAME => 'MENU_ITEM_NAME',
    TIMESTAMP_COLNAME => 'TIMESTAMP',
    TARGET_COLNAME => 'TOTAL_SOLD',
    CONFIG_OBJECT => {'prediction_interval': 0.95}
);


-- Create a table from the results
CREATE OR REPLACE TABLE vancouver_anomalies AS (
    SELECT *
    FROM TABLE(RESULT_SCAN(-1))
);

-- Review the results
SELECT * FROM vancouver_anomalies;

-- Query to identify trends
SELECT series, is_anomaly, count(is_anomaly) AS num_records
FROM vancouver_anomalies
WHERE is_anomaly =1
GROUP BY ALL
ORDER BY num_records DESC
LIMIT 5;
