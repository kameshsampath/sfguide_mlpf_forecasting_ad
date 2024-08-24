USE DATABASE quickstart;
USE SCHEMA ml_functions;

-- Note, your database and schema context for this table was set on page 2 step 2
-- query a sample of the ingested data
SELECT *
    FROM tasty_byte_sales
    WHERE menu_item_name LIKE 'Lobster Mac & Cheese';

-- Create table containing the latest years worth of sales data: 
CREATE OR REPLACE TABLE vancouver_sales AS (
    SELECT
        to_timestamp_ntz(date) AS timestamp,
        primary_city,
        menu_item_name,
        total_sold
    FROM
        tasty_byte_sales
    WHERE
        date > (SELECT max(date) - interval '1 year' FROM tasty_byte_sales)
    GROUP BY
        all
);

-- Create view for lobster sales
CREATE OR REPLACE VIEW lobster_sales AS (
    SELECT
        timestamp,
        total_sold
    FROM
        vancouver_sales
    WHERE
        menu_item_name LIKE 'Lobster Mac & Cheese'
);

-- Build Forecasting model; this could take ~15-25 secs; please be patient
CREATE OR REPLACE forecast lobstermac_forecast (
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'lobster_sales'),
    TIMESTAMP_COLNAME => 'TIMESTAMP',
    TARGET_COLNAME => 'TOTAL_SOLD'
);

-- Show models to confirm training has completed
SHOW forecast;


-- Create predictions, and save results to a table:  
CALL lobstermac_forecast!FORECAST(FORECASTING_PERIODS => 10);


-- Run immediately after the above call to store results!
CREATE OR REPLACE TABLE macncheese_predictions AS (
    SELECT
        *
    FROM
        TABLE(RESULT_SCAN(-1))
);

select * from macncheese_predictions;

-- Visualize the results, overlaid on top of one another: 
SELECT
    timestamp,
    total_sold,
    NULL AS forecast
FROM
    lobster_sales
WHERE
    timestamp > '2023-03-01'
UNION
SELECT
    TS AS timestamp,
    NULL AS total_sold,
    forecast
FROM
    macncheese_predictions
ORDER BY
    timestamp asc;


-- Run the forecast; this could take <15 secs
CALL lobstermac_forecast!FORECAST(FORECASTING_PERIODS => 10, CONFIG_OBJECT => {'prediction_interval': .5});