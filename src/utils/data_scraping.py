import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import BytesIO
from zipfile import ZipFile
from datetime import datetime

# Step 1
# Get a list of monthly pages for the sickness page
# Step 2
# Get a list of files for a given page
# Step 3
# Download a specified file

#Get the n most recent pages from the specified nhsd page
def get_last_n_pages(n, nhsd_publication,
                     url="https://digital.nhs.uk", 
                     section="/data-and-information/publications/statistical/"):
    pages = []

    url_full = url + section + nhsd_publication + "/"

    res = requests.get(url_full)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, 'html.parser')

    ls_div = soup.find(id="latest-statistics")
    pages.append(ls_div.a.get("href"))

    if n == 1:
        return pages
    
    pp_div = soup.find(id="past-publications")
    pp_pages = pp_div.find_all("a", attrs={"class": "cta__button"})[:n-1]

    for page in pp_pages:
        pages.append(page.get("href"))

    return pages

def get_files_from_page(page, url="https://digital.nhs.uk"):

    full_url = url + page

    response = requests.get(full_url)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, 'html.parser')
    file_div = soup.find(id="resources")


    relevant_files = {}
    for a_tag in file_div.find_all("a"):
        href = a_tag['href']
        if href:
            filename = href.split("/")[-1]
            filename_clean = filename.replace("%20", " ").replace("%2C", ",")
            #print(filename_clean)
            try:
                file_id, period_ext = filename_clean.rsplit(",", 1)
                period_ext_arr = period_ext.split(".")
                relevant_files[file_id] = {"url":href, 
                                           "period":period_ext_arr[0],
                                           "ext":period_ext_arr[1]}
            except:
                file_id_ext = filename_clean.split(".")
                relevant_files[file_id_ext[0]] = {"url":href, 
                                                  "ext":file_id_ext[1]}
                
    return relevant_files

def download_file_from_id(page_links, file_id):
    target_url = page_links[file_id]["url"]

    res = requests.get(target_url)
    if res.status_code == 200:
        return res.content
    else:
        print(f"Failed to download file with the following url:\n{target_url}.",
              f"\nStatus code: {res.status_code}")
        return 0

def save_file(content, page_links, file_id, dest_dir):
    target_link = page_links[file_id]
    file_period = target_link["period"]
    file_ext = target_link["ext"]

    target_dest = dest_dir + file_id + " -" + file_period + "." + file_ext
    
    with open(target_dest, "wb") as file:
        file.write(content)

def data_scrape(publication_name, target_files, 
                dest_dir="./data/", mode="latest", mode_n=1, con_debug=True):
    if con_debug:
        print("Data scraping start...")
    
    if mode == "latest":
        pages = get_last_n_pages(mode_n, publication_name)
    else:
        raise Exception(f"The data scraping mode {mode} is not supported.")

    for page in pages:
        if con_debug:
            print(page)
        
        res_page_links = get_files_from_page(page)

        for target in target_files:
            res_file = download_file_from_id(res_page_links, target)
        
            if res_file:
                save_file(res_file, res_page_links, target, "./data/current/")