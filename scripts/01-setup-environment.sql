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


Online documentation : https://docs.snowflake.net/manuals/user-guide/data-pipelines-intro.html

Data pipelines automate many of the manual steps involved in transforming and optimizing continuous data loads.
Frequently, the “raw” data is first loaded temporarily into a staging table used for interim storage and then
transformed using a series of SQL statements before it is inserted into the destination reporting tables.
The most efficient workflow for this process involves transforming only data that is new or modified.


we are going to create the following Data Pipeline :
  1- Manually ingest JSON files to a raw_json_table table without any transformation (only 1 VARIANT column)
  2- Extract RAW json data from raw_json_table and structure into a destination table (transformed_json_table)
  3- Aggregate data from transformed_json_table to a final destination table (final_table)

We will use 2 Warehouses :
  - load_wh for loading JSON data
  - task_wh to run the stored procedure that extract and transform the data between tables

This continuous Data Pipeline is incremental (no need to reload the entire table on every update) thanks to our Change Data Capture feature (CDC) :
  - changes over raw_json_table are tracked by the stream raw_data_stream
  - changed over transformed_json_table are tracked by the stream transformed_data_stream

All extract and transform operation are going to be processed through Stored Procedure called by a scheduled task :
  - The root task extract_json_data is scheduled every minute and call stored_proc_extract_json()
    - This stored procedure will look for INSERTS (from raw_data_stream) on the raw_json_table and then consume it to extract JSON data to transformed_json_table
  - The second task (aggregate_final_data) is not scheduled, as a workflow it run just after extract_json_data to call stored_proc_aggregate_final()
    - This stored procedure will look for INSERTS (from transformed_data_stream) on the transformed_json_table and then consume it to transform data to final_table

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
