USE ROLE ACCOUNTADMIN;

USE DATABASE GRADER_SETUP;

-- Run Grading
EXECUTE IMMEDIATE FROM @GRADER_SETUP.PUBLIC.builder_workshops/branches/main/gen-ai/tests.sql;