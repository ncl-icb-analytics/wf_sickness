import pandas as pd
import numpy as np
import os
import re
import toml

from datetime import datetime
from dotenv import load_dotenv
from os import getenv
from sqlalchemy import create_engine, MetaData, text, insert
from sqlalchemy.orm import sessionmaker

#Functions

#Return an object containing all runtime settings
def load_settings():
    #Load env settings
    load_dotenv(override=True)

    #Load toml settings from config
    config = toml.load("./config.toml")

    settings = {
        #SQL Connection settings
        "sql_address": getenv("SQL_ADDRESS"),
        "sql_database": config["database"]["sql_database"],
        "sql_schema": config["database"]["sql_schema"],
        "sql_table_sickness": config["database"]["sql_table_sickness"],
        "sql_table_byreason": config["database"]["sql_table_byreason"],
        "sql_cooloff": config["database"]["sql_cooloff"],

        #Volatile user settings
        "scrape_new_data": True if (
            getenv("SOURCE_SCRAPE") and getenv("SOURCE_SCRAPE") != "False"
            ) else False,
        "filename_cleanse": True if (
            getenv("SOURCE_CLEANSE") and getenv("SOURCE_CLEANSE") != "False"
            ) else False,
        "data_archive": True if (
            getenv("SOURCE_ARCHIVE") and getenv("SOURCE_ARCHIVE") != "False"
            ) else False,
        
        #Structure information
        "source_directory": ("./" + config["struct"]["data_dir"] + 
                             "/" + config["struct"]["source_dir"] + "/"),
        "archive_directory": ("./" + config["struct"]["data_dir"] + "/" + 
                              config["struct"]["archive_dir"] + "/"),

        #Lookup / reference values
        "map_column": config["map_files"]["column_names"],
        "region_code_london": config["codes"]["region_london"]
    }

    return settings

#Use data scraping to fetch files directly from NHSD
def scrape_new_data(settings):
    pass

#Function that renames the source file with a more appropiate filename
def filename_cleanse(old_filename, settings):

    #Get shortpath
    src = settings["source_directory"]

    #Determine file type (using filename, relies on assumption)
    if "reason" in old_filename.lower():
        file_type = "ByReason"
    else:
        file_type = "Benchmarking"

    #Extract year and month from the file name
    match = re.search(r'\b\d{4}\b', old_filename)
    if not match:
        raise Exception((f"Please ensure the source file {old_filename} "
                         "has a proper name.\nThe source should contain a month"
                         " and year in the form of 'March 2024' or '03 2024'"
                         "\n Full details on source filenames are found in the " 
                         "README.md file"))
    
    fn_year = match.group()
    fn_year_idx = old_filename.find(fn_year)

    #Extract the month from the file name
    ## If the filename has already been cleansed then the month is after
    if old_filename[fn_year_idx-2] == "-":
        fn_month = old_filename[fn_year_idx + 5:].split(" ")[0][:2]
    ## If the filename has not been cleansed then the month is before
    else:
        fn_month = old_filename[:fn_year_idx-1].split(" ")[-1]
    
    month_dict = {
    "January": "01", "February": "02", "March": "03",
    "April": "04", "May": "05", "June": "06",
    "July": "07", "August": "08", "September": "09",
    "October": "10", "November": "11", "December": "12"
    }

    if fn_month in month_dict.keys():
        fn_month = month_dict[fn_month]

    if fn_month not in month_dict.values():
        raise Exception((f"Please ensure the source file {old_filename} "
                         "has a proper name.\nThe source should contain a month"
                         " and year in the form of 'March 2024' or '03 2024'"
                         "\n Full details on source filenames are found in the " 
                         "README.md file"))
    
    new_filename = f"Sickness {file_type} - " + fn_year +" "+ fn_month + ".csv"
    os.rename(src + old_filename, src + new_filename)

    return new_filename

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
def get_source_files(settings):
    #Get all files in the source data directory
    data_dir = settings["source_directory"]
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
            csv_files.append(sf)

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

    #Replace NaNs with None
    df = df.replace({np.nan: None})

    #Fix issue with RNOH and CNWL ics_code
    #In some NHSE datasets, RNOH is labelled as NWL and CNWL is labelled as NCL
    df.loc[df["org_code"] == "RAN", "ics_code"] = "QMJ"
    df.loc[df["org_code"] == "RAN", "ics_name"] = "North Central London"
    df.loc[df["org_code"] == "RV3", "ics_code"] = "QRV"
    df.loc[df["org_code"] == "RV3", "ics_name"] = "North West London"

    #Add a current timestamp to the data
    df["date_upload"] = datetime.today()

    #Convert existing date column to date object
    df["date_data"] = pd.to_datetime(df["date_data"], dayfirst=True)
    
    return df

#Function to upload data for a given dataset
def upload_data(sf, df, dataset, settings):

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
    batch_size = 200
    
    ##Delete existing overlapping data to allow for re-uploading
    data_daterange = df["date_data"].unique()
    if len(data_daterange) > 1:
        raise Warning(("Multiple dates were found in the data.\n"
                      "This process does not replace existing data in the "
                      "destination for files with multiple dates."))
            
    rows = df.to_dict(orient="records")

    metadata = MetaData(schema=sql_schema)
    metadata.reflect(bind=engine)
    sqlalc_table = metadata.tables[sql_schema + '.' + sql_table]

    with engine.connect() as con:

        #If there is only a single date in the data
        if len(data_daterange) == 1:
            #Create delete query to remove existing data for this data point
            del_query = (f"DELETE FROM "
                        f"[{sql_database}].[{sql_schema}].[{sql_table}] "
                        f"WHERE date_data = '{data_daterange[0]}'")
    
            #Delete existing data from the destination
            con.execute(text(del_query))
        
        #Group the data into batches and upload
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            result = con.execute(
                insert(sqlalc_table),
                batch
            )

        con.commit()

    #Archive the file after upload if enabled
    if settings["data_archive"]:
        active_dir = settings["source_directory"]
        archive_dir = settings["archive_directory"]
        os.rename(active_dir + sf, archive_dir + sf)

#Load the runtime settings
settings = load_settings()

#Extract the data from the source
##If enabled, scrape new data from NHSD
if settings["scrape_new_data"]:
    scrape_new_data(settings)

##Get the datafile(s)
source_files = get_source_files(settings)

for sf in source_files:

    if settings["filename_cleanse"]:
        filename = filename_cleanse(sf, settings)
    else:
        filename = sf

    print(filename)

    #Load the data
    df_source = pd.read_csv(settings["source_directory"] + sf)

    #Transform the data
    df_processed = process_benchmarking_data(df_source, settings)

    #Load the data into the warehouse
    upload_data(filename, df_processed, "sickness", settings)