/* ##########################################################################################################################

This script is used for staging the sample shape zip file to the internal named stage GEO_STAGE.

Full GitHub project available here : https://github.com/apr4th/geo-data-ingestion-over16mb/tree/main

Mohanraj Palaniswamy - Sr. Solutions Architect
mohanraj.palaniswamy@snowflake.com

########################################################################################################################## */

--
-- Setting the Context
--
USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE GEOGRAPHIC;
USE SCHEMA SHAPES;


--
-- Put a sample Shape Zip File To Stage: GEO_STAGE
-- Note: 
-- Run the following PUT command in snowsql to stage the sample shape zip file to GEO_STAGE.
-- Replace <<folder_path>> and <<file_name>> with actual values
-- For Ex: /tmp/shapefiles and coffee_county.zip
-- Repeat the same command to load multiple shape files
--
PUT 'file://<<folder_path>>/<<file_name>>' @GEO_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
