import pandas as pd
import os
import toml

from datetime import datetime
from dotenv import load_dotenv
from os import getenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

#Functions

#Return an object containing all runtime settings
def load_settings():
    #Load env settings
    load_dotenv(override=True)

    #Load toml settings from config
    config = toml.load("./config.toml")

    settings = {

        "sql_address": getenv("SQL_ADDRESS"),
        "sql_database": config["database"]["sql_database"],
        "sql_schema": config["database"]["sql_schema"],
        "sql_table_sickness": config["database"]["sql_table_sickness"],
        "sql_table_byreason": config["database"]["sql_table_byreason"],
        "sql_cooloff": config["database"]["sql_cooloff"],

        "map_column": config["map_files"]["column_names"],
        "region_code_london": config["codes"]["region_london"]
    }

    return settings

#Establish a connection to the database
def db_connect(server_address, database, 
               server_type="mssql", driver="SQL+Server"):
    
    #Create Connection String
    conn_str = (f"{server_type}://{server_address}/{database}"
                f"?trusted_connection=yes&driver={driver}")
    
    #Create SQL Alchemy Engine object
    engine = create_engine(conn_str, use_setinputsizes=False)
    return engine

#Return a list of all csv files in the data/current directory
def get_source_files():
    #Get all files in the source data directory
    data_dir = "./data/current/"
    dir_list = os.listdir(data_dir)

    #Cleanse the list
    if dir_list == []:
        raise Exception(
            ("\n\nNo files were found."
             "\nThe NHSD data should be saved in the "
             f"'{os.path.abspath(data_dir)}' directory.")
        )

    #Ensure all data is a csv file
    csv_files = []
    for sf in dir_list:
        if not(sf.endswith(".csv")):
            print(f"Warning: {sf} is not a csv file and will not be processed.")
        else:
            csv_files.append(data_dir + sf)

    return csv_files

#The NHSD Data files change over time
def process_benchmarking_data(df_in, settings):
 
    #Work using a clean copy of the data frame
    df = df_in.copy()

    #Map column names (and drop unused columns)
    ##Load the map file
    dir_map = settings["map_column"]
    df_map = pd.read_csv(dir_map)
    ##Apply the map on the column names
    df.rename(columns=df_map.set_index("source_name")["output_name"], 
              inplace=True)
    ##Remove unused columns (columns not specified in the map file)
    df = df[df_map["output_name"].values]
    

    #Filter to London only
    region_code = settings["region_code_london"]
    df = df[df["region_code"] == region_code]

    #Drop the region_code column
    df.drop("region_code", axis=1, inplace=True)

    #Add a current timestamp to the data
    df["date_upload"] = datetime.today()

    #Convert existing date column to date object
    df["date_data"] = pd.to_datetime(df["date_data"], dayfirst=True)
    
    return df

#Function to upload data for a given dataset
def upload_data(df, dataset, settings):

    #Load destination table name
    try:
        sql_table = settings["sql_table_" + dataset]
    except:
        raise Exception((f"'sql_table_{dataset}' was not found in the"
                          "'[database]' section of the config.toml file."))

    #Set up Database Connection
    server_address = settings["sql_address"]
    sql_database = settings["sql_database"]

    #Connect to the database
    engine = db_connect(server_address, sql_database)

    #Upload the processed data
    sql_schema = settings["sql_schema"]
    
    ##Delete existing overlapping data to allow for re-uploading
    data_daterange = df["date_data"].unique()
    if len(data_daterange) > 1:
        raise Warning(("Multiple dates were found in the data.\n"
                      "This process does not replace existing data in the "
                      "destination for files with multiple dates."))
    
    if len(data_daterange) == 1:
        del_query = (f"DELETE FROM "
                     f"[{sql_database}].[{sql_schema}].[{sql_table}] "
                     f"WHERE date_data = '{data_daterange[0]}'")
        

    ##Upload the processed data
    df.to_sql(sql_table, engine, schema=sql_schema, 
              if_exists="append", index=False, method="multi",
              chunksize=200)


#Load the runtime settings
settings = load_settings()

#Extract the data from the source
##Get the datafile(s)
source_files = get_source_files()

for sf in source_files:

    #Load the data
    df_source = pd.read_csv(sf)

    #Transform the data
    df_processed = process_benchmarking_data(df_source, settings)

    #Load the data into the warehouse
    upload_data(df_processed, "sickness", settings)