import pandas as pd
import numpy as np
import os
import re
import toml
import tkinter as tk

from datetime import datetime
from dotenv import load_dotenv
from os import getenv
from sqlalchemy import create_engine, MetaData, text, insert
from sqlalchemy.orm import sessionmaker
from tkinter import messagebox

from utils.data_scraping import *

##Global Variables
overwrite_warning = True
overwrite = True

##Functions

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
        "overwrite_warning": True if (
            getenv("OVERWRITE_WARN") and getenv("OVERWRITE_WARN") != "False"
            ) else False,
        "overwrite_default": True if (
            getenv("OVERWRITE_DEFAULT") 
            and getenv("OVERWRITE_DEFAULT") != "False"
            ) else False,

        #Structure information
        "source_directory": ("./" + config["struct"]["data_dir"] + 
                             "/" + config["struct"]["source_dir"] + "/"),
        "archive_directory": ("./" + config["struct"]["data_dir"] + "/" + 
                              config["struct"]["archive_dir"] + "/"),

        #Lookup / reference values
        "map_column": config["map_files"]["column_names"],
        "ics_lookup": config["map_files"]["ics_lookup"],
        "region_code_london": config["codes"]["region_london"],

        #Data scraping settings
        "scrape_mode": getenv("SOURCE_SCRAPE_MODE").lower(),
        "publication_name": config["data_scraping"]["publication_name"],
        "target_files": config["data_scraping"]["target_files"]
    }

    return settings

#Use data scraping to fetch files directly from NHSD
def scrape_new_data(settings):
    target_publicaton = settings["publication_name"]
    target_files = settings["target_files"]
    target_dir = settings["source_directory"]
    scrape_mode = settings["scrape_mode"]

    #Parse the specified SOURCE_SCRAPE_MODE
    #Check if the given value is in the form "mode n" and if not set n to 1.
    if len(scrape_mode.split(" ")) > 1:
        mode_type, mode_n = scrape_mode.split(" ")
    else:
        mode_type = scrape_mode
        mode_n = 1

    #Call the data scraping code
    data_scrape(publication_name=target_publicaton, 
                target_files=target_files, 
                dest_dir=target_dir,
                mode=mode_type,
                mode_n=mode_n,
                con_debug=True)

#Function that renames the source file with a more appropiate filename
def filename_cleanse(old_filename, file_type, settings):

    #Get shortpath
    src = settings["source_directory"]

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

    #If the month is written as the word, map it to the numeric value
    if fn_month in month_dict.keys():
        fn_month = month_dict[fn_month]

    #If no valid month is found
    if fn_month not in month_dict.values():
        raise Exception((f"Please ensure the source file {old_filename} "
                         "has a proper name.\nThe source should contain a month"
                         " and year in the form of 'March 2024' or '03 2024'"
                         "\n Full details on source filenames are found in the " 
                         "README.md file"))
    
    #Derrive the cleansed file name
    new_filename = f"Sickness {file_type} - " + fn_year +" "+ fn_month + ".csv"
    
    #If the file was already cleansed, no need to check for filename conflicts
    if old_filename == new_filename:
        return old_filename

    #Make sure the new file name does not already exist as a source file
    #In this case ignored the uncleansed source file as the code has no way of
    #prioritising overlapping data beyond which was processed most recently.
    if os.path.isfile(src + new_filename):
        print(f"Warning! {old_filename} will be renamed to {new_filename}",
              " but this already exists. {old_filename} will not be processed",
              " as the code does not know how to handle both.")
        return False
    else:
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

    #Validate each source file
    for sf in dir_list:
        if not(sf.endswith(".csv")):
            print(f"Warning: {sf} is not a csv file and will not be processed.")
        else:
            csv_files.append(sf)

    return csv_files

#The NHSD Data files change over time
def process_benchmarking_data(df_in, file_type, ics_lookup, settings):
 
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
    df = df[df.columns.intersection(df_map["output_name"].values)]

    #Filter to London only
    region_code = settings["region_code_london"]
    df = df[df["region_code"] == region_code]

    #Drop the region_code column
    df.drop("region_code", axis=1, inplace=True)

    #Replace Nans with None
    df = df.replace({np.nan: None})

    #By Reason specific processing
    if file_type == "ByReason":
        #Split reason_full coloumn into code and description
        df["reason_code"] = df["reason_full"].str[0:3]
        df["reason_desc"] = df["reason_full"].str[4:]
        df.drop(["reason_full"], axis=1, inplace=True)

    #Add ics columns using the dictionary table
    df = df.join(ics_lookup.set_index("org_code"), on="org_code")

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

#Function to get the ICS mapping information
def get_ics_lookup(settings):
   
    #Set up Database Connection
    server_address = settings["sql_address"]
    sql_database = settings["sql_database"]

    #Connect to the database
    engine = db_connect(server_address, sql_database)
    with engine.connect() as con:
        
        #Load the ICS Lookup sql script and store the results
        with open(settings["ics_lookup"]) as file:
            sfw_query = text(file.read())
            df_out = pd.read_sql_query(sfw_query, con)

    df_out.drop("org_name", axis=1, inplace=True)

    return df_out

#Prompt the user to decide how to handle file name conflicts when archiving.
def overwrite_prompt(filename):
    
    root = tk.Tk()
    root.withdraw()

    # Create a new top-level window
    dialog = tk.Toplevel(root)
    dialog.title("File already exists")
    #dialog.geometry("500x150")

    popup_font = ("Arial", 12)

    # Add a description label
    desc_text = f"""The file: "{filename}" already exists in the archive folder. 
    \nDo you want to overwrite it?"""
    label = tk.Label(dialog, text=desc_text, font=popup_font)
    label.pack(padx = 20, pady=10)

    # Variable to store checkbox state
    apply_to_all_var = tk.BooleanVar()

    # Add a checkbox
    checkbox = tk.Checkbutton(dialog, 
                              text="Apply my choice to all future conflicts", 
                              variable=apply_to_all_var,
                              font=popup_font)
    checkbox.pack(pady=5)

    # Variable to store the user's choice
    result = tk.StringVar(value="")

    # Function to handle button click and set result
    def on_button_click(choice):
        result.set(choice)
        dialog.destroy()

    # Create Yes and No buttons
    yes_button = tk.Button(dialog, 
                           text="Yes", 
                           command=lambda: on_button_click(True),
                           font=popup_font,
                           height=1, width = 8)
    no_button = tk.Button(dialog, 
                          text="No", 
                          command=lambda: on_button_click(False),
                          font=popup_font,
                          height=1, width = 8)
    

    # Pack the buttons
    yes_button.pack(side="left", padx=50, pady=15)
    no_button.pack(side="right", padx=50, pady=15)

    # Allow the window to resize based on its content
    dialog.update_idletasks()
    dialog.resizable(False, False)  

    # Wait for the dialog to close
    dialog.wait_window()

    #Check if the user used the "Apply to all" option
    global overwrite_warning
    overwrite_warning = not apply_to_all_var.get()

    # Return the result and the checkbox state
    return bool(int(result.get()))

#Function that handles the file archiving
def archive_file(filename, settings):

    #Load settings
    active_dir = settings["source_directory"]
    archive_dir = settings["archive_directory"]

    #Check for conflict
    file_source = active_dir + filename
    file_dest = archive_dir + filename

    ##Conflict found
    if os.path.isfile(file_dest):
        #Check if the user should be prompted
        global overwrite
        if overwrite_warning:
            overwrite = overwrite_prompt(filename)

        #Check if the overwrite should occur
        if overwrite:
            os.remove(file_dest)
            os.rename(file_source, file_dest)

    ##No conflict, can archive as normal 
    else:
        #Archive the file
        os.rename(file_source, file_dest)

#Function to upload data for a given dataset
def upload_data(sf, df, dataset, settings):

    #Load destination table name
    try:
        sql_table = settings["sql_table_" + dataset.lower()]
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
        archive_file(sf, settings)

#Load the runtime settings
settings = load_settings()

#Extract the data from the source
##If enabled, scrape new data from NHSD
if settings["scrape_new_data"]:
    scrape_new_data(settings)

#Set the overwrite settings
overwrite_warning = settings["overwrite_warning"]
if not overwrite_warning:
    overwrite = settings["overwrite_default"]

##Get the datafile(s)
source_files = get_source_files(settings)

#Load the ICS lookup
ics_lookup = get_ics_lookup(settings)

print("\nBegin processing...")

for sf in source_files:

    #Determine file type (using filename, relies on assumption)
    if "reason" in sf.lower():
        file_type = "ByReason"
        file_cleanse = "by Reason"
    else:
        file_type = "Sickness"
        file_cleanse = "Benchmarking"

    if settings["filename_cleanse"]:
        filename = filename_cleanse(sf, file_cleanse, settings)
    else:
        filename = sf

    #Check there was no conflict issue within the source data
    ##(This can happen when attempting to cleasne the filename of a source file
    ##and its new name matches another source file)
    if filename:
        print(filename)

        #Load the data
        df_source = pd.read_csv(settings["source_directory"] + filename)

        #Transform the data
        df_processed = process_benchmarking_data(
            df_source, file_type, ics_lookup, settings)

        #Load the data into the warehouse
        upload_data(filename, df_processed, file_type, settings)

print("\nFinished processing.\n")