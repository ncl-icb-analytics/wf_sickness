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
def get_last_n_pages(n, nhsd_page,
                     url="https://digital.nhs.uk", 
                     section="/data-and-information/publications/statistical/"):
    pages = []

    url_full = url + section + nhsd_page + "/"

    res = requests.get(url_full)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, 'html.parser')

    ls_div = soup.find(id="latest-statistics")
    pages.append(ls_div.a.get("href"))

    if n == 1:
        return pages
    
    pp_div = soup.find(id="past-publications")
    pp_pages = pp_div.find_all("a")[:n-1]

    for page in pp_pages:
        pages.append(page.get("href"))

    return pages

print(get_last_n_pages(4, "nhs-sickness-absence-rates"))