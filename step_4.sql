USE DATABASE quickstart;
USE SCHEMA ml_functions;

-- Create a view for the holidays in Vancouver, which is located in British Columbia (BC) in Cananda (CA)
CREATE OR REPLACE VIEW canadian_holidays AS (
    SELECT
        date,
        holiday_name,
        is_financial
    FROM
        frostbyte_cs_public.cybersyn.public_holiday_calendar
    WHERE
        ISO_ALPHA2 LIKE 'CA'
        AND date > '2022-01-01'
        AND (
            subdivision IS null
            OR subdivision LIKE 'BC'
        )
    ORDER BY
        date ASC
);

-- Verify the data
select * from canadian_holidays;

-- Create a view for our training data, including the holidays for all items sold
CREATE OR REPLACE VIEW allitems_vancouver AS (
    SELECT
        vs.timestamp,
        vs.menu_item_name,
        vs.total_sold,
        ch.holiday_name
    FROM 
        vancouver_sales vs
        LEFT JOIN canadian_holidays ch ON vs.timestamp = ch.date
    WHERE MENU_ITEM_NAME IN ('Mothers Favorite', 'Bottled Soda', 'Ice Tea')
);


-- Train Model; this could take ~15-25 secs; please be patient
CREATE OR REPLACE forecast vancouver_forecast (
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'allitems_vancouver'),
    SERIES_COLNAME => 'MENU_ITEM_NAME',
    TIMESTAMP_COLNAME => 'TIMESTAMP',
    TARGET_COLNAME => 'TOTAL_SOLD'
);

-- show it
SHOW forecast;


-- Retrieve the latest date from our input dataset, which is 05/28/2023: 

SELECT MAX(timestamp) FROM vancouver_sales;


-- Create view for inference data
CREATE OR REPLACE VIEW vancouver_forecast_data AS (
    WITH future_dates AS (
        SELECT
            '2023-05-28' ::DATE + row_number() OVER (
                ORDER BY
                    0
            ) AS timestamp
        FROM
            TABLE(generator(rowcount => 10))
    ),
    food_items AS (
        SELECT
            DISTINCT menu_item_name
        FROM
            allitems_vancouver
    ),
    joined_menu_items AS (
        SELECT
            *
        FROM
            food_items
            CROSS JOIN future_dates
        ORDER BY
            menu_item_name ASC,
            timestamp ASC
    )
    SELECT
        jmi.menu_item_name,
        to_timestamp_ntz(jmi.timestamp) AS timestamp,
        ch.holiday_name
    FROM
        joined_menu_items AS jmi
        LEFT JOIN canadian_holidays ch ON jmi.timestamp = ch.date
    ORDER BY
        jmi.menu_item_name ASC,
        jmi.timestamp ASC
);

-- Call the model on the forecast data to produce predictions: 
CALL vancouver_forecast!forecast(
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'vancouver_forecast_data'),
        SERIES_COLNAME => 'menu_item_name',
        TIMESTAMP_COLNAME => 'timestamp'
);

-- Store results into a table: 
CREATE OR REPLACE TABLE vancouver_predictions AS (
    SELECT *
    FROM TABLE(RESULT_SCAN(-1))
);

select * FROM vancouver_predictions;

-- get Feature Importance
CALL VANCOUVER_FORECAST!explain_feature_importance();

-- View data  - none of the sales date overlap with any holidays thereby voiding the impact

select * from vancouver_forecast_data;

-- Evaluate model performance:
CALL VANCOUVER_FORECAST!show_evaluation_metrics();
