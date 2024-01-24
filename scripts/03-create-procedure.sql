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
-- Create a Procedure to Load the Shape Files
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

        # get the list of filenames from the zip file
        with zipfile.ZipFile(SnowflakeFile.open(p_stage_path, 'rb'), 'r') as zip:
            file_list = zip.namelist()

        # select the shape files from the file_list
        shp_file_list = [file for file in file_list if file.endswith('.shp')]

        out = ''
        total_rowcnt = 0
        # iterate through the shp_file_list to load
        for filename in shp_file_list:
            with SnowflakeFile.open(p_stage_path, 'rb') as f:
                with ZipMemoryFile(f) as zip:
                    with zip.open(filename) as collection:
                        progeo_df = pd.DataFrame()
                        progeoco_df = pd.DataFrame()

                        pro_cnt = 0
                        pro_list = []
                        for record in collection:
                            geo_str = str(shape(record['geometry']).wkt)

                            if (loads(geo_str).geom_type.upper() == "POLYGON"):
                                co_str = (list(loads(geo_str).exterior.coords))
                            else:
                                co_str = (list(loads(geo_str).coords))

                            co_cnt = 0
                            co_list = []
                            for co in co_str:
                               co_dict = {"ZIP_FILE_NAME":p_zipfile_name, "SHP_FILE_NAME":filename, "SHP_FILE_REC_SEQ":pro_cnt, 
                                          "COORDINATES_SEQ":co_cnt, "LONGITUDE":str(co[0]), "LATITUDE":str(co[1])};
                               co_list.append(co_dict)
                               co_cnt=co_cnt+1

                            temp_dict = str(dict(record['properties'])).replace('None', '\'None\'')
                            pro_dict = {"ZIP_FILE_NAME":p_zipfile_name, "SHP_FILE_NAME":filename, "SHP_FILE_REC_SEQ":pro_cnt, "PROPERTY":temp_dict};
                            # pro_dict.update(temp_dict)
                            pro_dict['GEOMETRY_TYPE'] = loads(geo_str).geom_type.upper()

                            progeo_df = progeo_df.append([pro_dict])
                            progeoco_df = progeoco_df.append(co_list)
                            pro_cnt = pro_cnt + 1 

                        total_rowcnt = total_rowcnt + pro_cnt
                        progeo_tbl_df = sp_session.write_pandas(progeo_df, p_table_name
                                                                         , quote_identifiers = False
                                                                         , auto_create_table = True
                                                                         , overwrite = False
                                                                         , table_type = 'transient')

                        progeoco_tbl_df = sp_session.write_pandas(progeoco_df, f'{p_table_name}_coordinates'
                                                                             , quote_identifiers = False
                                                                             , auto_create_table = True
                                                                             , overwrite = False
                                                                             , table_type = 'transient')

        out = 'Records inserted: ' + str(total_rowcnt)
        return out

    except Exception as error:
        return (str(error) + " in " + filename);
$$;


--
-- Load the Shape Files
--
CALL load_shapefile(BUILD_SCOPED_FILE_URL(@GEO_STAGE,'CoffeeCounty.zip'),'CoffeeCounty.zip','COFFEE_COUNTY');
CALL load_shapefile(BUILD_SCOPED_FILE_URL(@GEO_STAGE,'BaldwinCounty.zip'),'BaldwinCounty.zip','BALDWIN_COUNTY');
