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
                file_id, period = filename_clean.rsplit(",", 1)
            except:
                file_id = filename_clean.split(".")[0]

            relevant_files[file_id] = href

    return relevant_files

def download_file_from_id(page_links, file_id):
    target_url = page_links[file_id]
    print(target_url)

target_files = ["NHS Sickness Absence benchmarking tool CSV", 
           "NHS Sickness Absence by reason, staff group and organisation CSV"]

pages = get_last_n_pages(1, "nhs-sickness-absence-rates")
#pages = get_last_n_pages(4, "general-and-personal-medical-services")

for page in pages:
    print(page)
    res = get_files_from_page(page)

    for target in target_files:
        download_file_from_id(res, target)
    
