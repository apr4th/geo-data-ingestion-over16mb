/* ##########################################################################################################################

Script Files to set up the Snowflake environment and create necessary database objects to ingest all geometric records,
encompassing the coordinates dataset that are larger than the 16MB size, from every Shape file contained within a zip file.

Full GitHub project available here : https://github.com/apr4th/geo-data-ingestion-over16mb/tree/main

In this tutorial, the procedure load_shapefile creates two tables for each zip file that you want to ingest:
1. The parent table stores all the geometric properties including the shape type
2. The child table stores every coordinates (longitude and latitude) as a single record

Some important useful Snowflake document links for more information:
--------------------------------------------------------------------
Create Database: https://docs.snowflake.com/en/sql-reference/sql/create-database
Create Procedure: https://docs.snowflake.com/en/developer-guide/stored-procedure/stored-procedures-python
Create Schema: https://docs.snowflake.com/en/sql-reference/sql/create-schema
Create Stage: https://docs.snowflake.com/en/sql-reference/sql/create-stage
Build Scoped URL: https://docs.snowflake.com/en/sql-reference/functions/build_scoped_file_url
Put: https://docs.snowflake.com/en/sql-reference/sql/put

In this tutorial, we'll utilize the existing warehouse COMPUTE_WH. Ensure that you have this warehouse available in your 
platform or update the warehouse name in the script files if you prefer to use a different one.

Mohanraj Palaniswamy - Sr. Solutions Architect
mohanraj.palaniswamy@snowflake.com

########################################################################################################################## */

--
-- Setting the Context
--
USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;


--
-- Create a Repository Database: GEOGRAPHIC
--
DROP DATABASE IF EXISTS GEOGRAPHIC;

CREATE DATABASE IF NOT EXISTS GEOGRAPHIC
                              DATA_RETENTION_TIME_IN_DAYS = 7
                              COMMENT = 'GEOGRAPHIC Database to demonstrate Shape File Ingestion';


--
-- Create Schemas: SHAPES
--
USE DATABASE GEOGRAPHIC;

DROP SCHEMA IF EXISTS SHAPES;

CREATE SCHEMA IF NOT EXISTS SHAPES WITH MANAGED ACCESS
                            DATA_RETENTION_TIME_IN_DAYS = 3
                            COMMENT = 'SHAPES Schema to store Geographic Properties and Coordinates';


--
-- Create Named Internal Stage: GEO_STAGE
--
USE SCHEMA SHAPES;

DROP STAGE IF EXISTS GEO_STAGE;

CREATE STAGE IF NOT EXISTS GEO_STAGE
DIRECTORY = (ENABLE=TRUE);
