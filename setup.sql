-- Using accountadmin is often suggested for quickstarts, but any role with sufficient privledges can work
USE ROLE ACCOUNTADMIN;

-- Pull holiday data
-- Ensure that CyberSyn Global Government Data is setup 
-- IF not set it up via Marketplace --> CyberSyn Global Government
SHOW SHARES;

-- Create development database, schema for our work: 
CREATE OR REPLACE DATABASE quickstart;
CREATE OR REPLACE SCHEMA ml_functions;

-- Use appropriate resources: 
USE DATABASE quickstart;
USE SCHEMA ml_functions;

-- Create warehouse to work with: 
CREATE OR REPLACE WAREHOUSE quickstart_wh;
USE WAREHOUSE quickstart_wh;

-- Set search path for ML Functions:
-- ref: https://docs.snowflake.com/en/user-guide/ml-powered-forecasting#preparing-for-forecasting
ALTER ACCOUNT
SET SEARCH_PATH = '$current, $public, SNOWFLAKE.ML';

-- TODO update references to right repo
CREATE OR REPLACE API INTEGRATION KAMESHSAMPATH_GIT_API_INTEGRATION
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/kameshsampath')
  ENABLED = TRUE;

CREATE GIT REPOSITORY snowflake_labs_mlpf_forecasting_ad 
	ORIGIN = 'https://github.com/kameshsampath/sfguide_mlpf_forecasting_ad' 
	API_INTEGRATION = 'KAMESHSAMPATH_GIT_API_INTEGRATION';

-- refresh and fetch 

ALTER GIT REPOSITORY snowflake_labs_mlpf_forecasting_ad FETCH;

-- make sure you get tests.sql and setup.sql files
ls @snowflake_labs_mlpf_forecasting_ad/branches/main;




