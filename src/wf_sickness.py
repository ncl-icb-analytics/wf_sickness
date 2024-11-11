import pandas as pd
import os
import toml

from datetime import datetime
from dotenv import load_dotenv
from os import getenv

#Functions

def load_settings():
    #Load env settings
    load_dotenv(override=True)

    #Load toml settings from config
    config = toml.load("./config.toml")

    settings = {

        "sql_database": config["database"]["sql_database"],
        "sql_schema": config["database"]["sql_schema"],
        "sql_table_sickness": config["database"]["sql_table_sickness"],
        "sql_table_byreason": config["database"]["sql_table_byreason"],
        "sql_cooloff": config["database"]["sql_cooloff"],

        "map_column": config["map_files"]["column_names"],
        "region_code_london": config["codes"]["region_london"]
    }

    return settings

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
    df["date_extract"] = datetime.today()
    
    return df

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

    print(df_processed.head())

    #Load the data into the warehouse