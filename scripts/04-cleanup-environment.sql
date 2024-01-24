/* ##########################################################################################################################

This script is used for cleaning up the environment after this tutorial is completed.

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
-- Cleanup the Environment
--
DROP STAGE IF EXISTS GEO_STAGE;
DROP SCHEMA IF EXISTS SHAPES;
DROP DATABASE IF EXISTS GEOGRAPHIC;
