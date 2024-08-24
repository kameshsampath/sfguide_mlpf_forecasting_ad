USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE DATABASE GRADER_SETUP;
USE DATABASE GRADER_SETUP;

CREATE OR REPLACE API INTEGRATION git_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/Snowflake-Labs')
  ENABLED = TRUE;

CREATE OR REPLACE GIT REPOSITORY builder_workshops
  API_INTEGRATION = git_api_integration
  ORIGIN = 'https://github.com/Snowflake-Labs/builder-workshops.git';

ALTER GIT REPOSITORY builder_workshops FETCH;

-- make sure you get tests.sql and setup.sql files
ls @builder_workshops/branches/main;

-- Setup Auto Grader
EXECUTE IMMEDIATE FROM @GRADER_SETUP.PUBLIC.builder_workshops/branches/main/auto-grader/setup.sql
    USING(email => 'kamesh.sampath@hotmail.com', first_name => 'Kamesh', middle_name => '' ,last_name => 'Sampath');