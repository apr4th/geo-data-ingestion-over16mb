/* ##########################################################################################################################

This script is used for creating the procedure load_shapefile to ingest the geospatial data from the sample shape zip file.

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
-- Create a Procedure to Load the Shape Files: load_shapefile
-- Arguments
-- [1] - Scoped URL of the Shape Zip File using BUILD_SCOPED_FILE_URL function
-- [2] - Name of the Shape Zip File
-- [3] - Table Name where you want to store the GEO Properties
--
DROP PROCEDURE IF EXISTS load_shapefile(varchar, varchar, varchar);

CREATE PROCEDURE IF NOT EXISTS load_shapefile(stage_path varchar, zipfile_name varchar, table_name varchar)
returns varchar
language python
runtime_version = 3.8
packages        = ('geopandas','pandas==1.5.3','snowflake-snowpark-python')
handler         = 'main'
execute as owner
as
$$
from fiona.io import ZipMemoryFile
from shapely.geometry import shape
from shapely.wkt import loads
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
import zipfile

def main(sp_session, p_stage_path, p_zipfile_name, p_table_name):

    try:
        # dropping the existing tables
        drop_table    = f'DROP TABLE IF EXISTS {p_table_name}'
        drop_table_co = f'DROP TABLE IF EXISTS {p_table_name}_COORDINATES'
        sp_session.sql(drop_table).collect()
        sp_session.sql(drop_table_co).collect()

        # get the list of shp filenames from the zip file
        with zipfile.ZipFile(SnowflakeFile.open(p_stage_path, 'rb'), 'r') as zip:
            shp_file_list = [file for file in zip.namelist() if file.endswith('.shp')]

        out = ''
        total_procnt = 0
        # iterate through the shp_file_list to ingest each shape file's geometric data into a snowflake table
        for filename in shp_file_list:
            with SnowflakeFile.open(p_stage_path, 'rb') as f:
                with ZipMemoryFile(f) as zip:
                    with zip.open(filename) as collection:
                        # initialize dataframes for collecting Geo Properties and Coordiniates
                        progeo_df = pd.DataFrame()
                        progeoco_df = pd.DataFrame()

                        shp_file_req_seq = 0
                        pro_list = []
                        # iterate through all the records in the shape file
                        for record in collection:
                            # get the geometric property
                            geo_str = str(shape(record['geometry']).wkt)

                            # get the geometric coordinates 
                            if (loads(geo_str).geom_type.upper() == "POLYGON"):
                                co_str = (list(loads(geo_str).exterior.coords))
                            else:
                                co_str = (list(loads(geo_str).coords))

                            # create a list of coordinates
                            co_list = []
                            coordinates_seq = 0
                            for co in co_str:
                               co_dict = {"ZIP_FILE_NAME":p_zipfile_name,
                                          "SHP_FILE_NAME":filename,
                                          "SHP_FILE_REC_SEQ":shp_file_req_seq, 
                                          "COORDINATES_SEQ":coordinates_seq,
                                          "LONGITUDE":str(co[0]),
                                          "LATITUDE":str(co[1])};
                               co_list.append(co_dict)
                               coordinates_seq = coordinates_seq + 1

                            # create a list of properties
                            pro_list = [{"ZIP_FILE_NAME":p_zipfile_name,
                                         "SHP_FILE_NAME":filename,
                                         "SHP_FILE_REC_SEQ":shp_file_req_seq,
                                         "PROPERTY":str(dict(record['properties'])).replace('None', '\'None\''),
                                         "GEOMETRY_TYPE":loads(geo_str).geom_type.upper()}];

                            # add the properties and coordinates to their respective dataframes
                            progeo_df = progeo_df.append(pro_list)
                            progeoco_df = progeoco_df.append(co_list)
                            shp_file_req_seq = shp_file_req_seq + 1 

                        # add the property count of each shape file to total property count
                        total_procnt = total_procnt + shp_file_req_seq

                        # write the properties to parent table
                        progeo_tbl_df = sp_session.write_pandas(progeo_df, p_table_name
                                                                         , quote_identifiers = False
                                                                         , auto_create_table = True
                                                                         , overwrite = False
                                                                         , table_type = 'transient')

                        # write the coordinates to child table
                        progeoco_tbl_df = sp_session.write_pandas(progeoco_df, f'{p_table_name}_coordinates'
                                                                             , quote_identifiers = False
                                                                             , auto_create_table = True
                                                                             , overwrite = False
                                                                             , table_type = 'transient')

        out = 'Properties inserted: ' + str(total_procnt)
        return out

    except Exception as error:
        return (str(error) + " in " + filename);
$$;



--
-- Load the Shape Files
--
CALL load_shapefile(BUILD_SCOPED_FILE_URL(@GEO_STAGE,'CoffeeCounty.zip'),'CoffeeCounty.zip','COFFEE_COUNTY');
CALL load_shapefile(BUILD_SCOPED_FILE_URL(@GEO_STAGE,'BaldwinCounty.zip'),'BaldwinCounty.zip','BALDWIN_COUNTY');
