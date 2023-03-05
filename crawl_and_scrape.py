import os
import urllib
import zipfile

from bs4 import BeautifulSoup
import requests
import csv
import time
import re
import pandas as pd
import math

# Hard coded parameters for president/secretary visits and pre-exising datasets
DATA_FOLDER = './data/'

# web-scrapping info
HEADER = "https://history.state.gov"
BODY_PRESIDENT = "/departmenthistory/travels/president/"
BODY_SECRETARY = "/departmenthistory/travels/secretary/"
URL_PRESIDENT = "https://history.state.gov/departmenthistory/travels/president"
URL_SECRETARY = "https://history.state.gov/departmenthistory/travels/secretary"
PRESIDENT_VISITS_FNAME = "AmericanPresidentVisit.csv"
SECRETARY_VISITS_FNAME = "AmericanSecretaryVisit.csv"
OUT_FILE_PRESIDENT = f"{DATA_FOLDER}{PRESIDENT_VISITS_FNAME}"
OUT_FILE_SECRETARY = f"{DATA_FOLDER}{SECRETARY_VISITS_FNAME}"

# existing csv/zip dataset links
COUNTRY_CODES_LINK = "https://correlatesofwar.org/wp-content/uploads/COW-country-codes.csv"
DIPLOMATIC_DATA_LINK = "https://correlatesofwar.org/wp-content/uploads/Diplomatic_Exchange_2006v1.csv"
POWER_DATA_LINK = "https://correlatesofwar.org/wp-content/uploads/NMC_Documentation-6.0.zip"
ECON_DATA_LINK = "https://dataverse.nl/api/access/datafile/354098"

# zip file of power data
POWER_DATA_ZIPFILE = POWER_DATA_LINK.split('/')[-1]

# csv dataset filenames
COW_COUNTRY_CODES_FNAME = COUNTRY_CODES_LINK.split('/')[-1]
DIPLOMATIC_DATA_FNAME = DIPLOMATIC_DATA_LINK.split('/')[-1]
ECONOMIC_DATA_FNAME = "pwt1001.dta"
POWER_DATA_FNAME = "NMC-60-abridged.csv"

# download pre-exising datasets
if not os.path.exists(DATA_FOLDER):
    # create directory to store data
    os.mkdir(DATA_FOLDER)
    for link, fname in zip([COUNTRY_CODES_LINK, DIPLOMATIC_DATA_LINK,
                            ECON_DATA_LINK],
                           [COW_COUNTRY_CODES_FNAME, DIPLOMATIC_DATA_FNAME,
                            ECONOMIC_DATA_FNAME]):
        response = requests.get(link)
        with open(f'{DATA_FOLDER}{fname}', "wb") as f:
            f.write(response.content)

    # get zipped power data
    urllib.request.urlretrieve(POWER_DATA_LINK,
                               f'{DATA_FOLDER}{POWER_DATA_ZIPFILE}')

    # extract zip power data in data folder
    with zipfile.ZipFile(POWER_DATA_ZIPFILE, 'r') as zip_ref:
        zip_ref.extractall(DATA_FOLDER)

    # extract again the unzipped files
    zip_files = [f for f in os.listdir(DATA_FOLDER)
                 if f.endswith('.zip') and f != POWER_DATA_ZIPFILE]
    for zip_file in zip_files:
        with zipfile.ZipFile(f'{DATA_FOLDER}{zip_file}', 'r') as zip_ref:
            zip_ref.extractall(DATA_FOLDER)

# COW mapping from country to code and code to country
with open(f"{DATA_FOLDER}{COW_COUNTRY_CODES_FNAME}", 'r') as f:
    COUNTRIES_TO_CODES_DICT = {row['StateNme']: int(row['CCode'])
                               for row in csv.DictReader(f)}
    CODES_TO_COUNTRIES_DICT = {val: k
                               for k, val in
                               COUNTRIES_TO_CODES_DICT.items()}


def get_travel_info(url_header, url_body, url, csv_filename):
    """
    Crawls and scrapes a US government website to get info on presidential
    and secretarial travels and creates a csv file with the scraped data.

    The format of the csv file:
        Column 1: Country
        Column 2: City
        Column 3: Description
        Column 4: Time

    Inputs:
        - url_header (str): The absolute part of the urls
                            e.g. https://history.state.gov
        - url_body (str): The relative part of the urls to be scraped
                          e.g. /departmenthistory/travels/president/
                          (relative to url_header)
        - url (str): The original "root" url that we start scraping
        - csv_filename (str): Name of the output csv file.

    Returns:
        None
    """
    soup = obtain_soup(url)
    urls_to_follow = find_urls_to_follow(soup, url_header, url_body)
    travel_data = crawl_and_scrape_travels(urls_to_follow)
    with open(csv_filename, 'w', encoding='utf-8', newline='') as f:
        write = csv.writer(f, delimiter=",")
        write.writerow(["destination country", "destination city",
                        "description", "time"])
        write.writerows(travel_data)
        f.close()


def obtain_soup(url):
    """
    Obtain the parsed bs4 result for a given url to be scraped.
    Input:
        - url (str)
    Returns:
         (BeatifulSoup) The parsed "soup" obtained by bs4
    """
    request = requests.get(url, headers=
    {'User-Agent': 'scraper for teaching bitsikokos@uchicago.edu'})
    request = request.text
    soup = BeautifulSoup(request, "html5lib")
    return soup


def find_urls_to_follow(soup, url_header, url_body):
    """
    Find the list of urls on presidential/secretaries visits

    Inputs:
     - soup (BeatifulSoup) the bs4 object for the starting url
     - url_header (str) the absolute part of the urls
     - url_body (str) relative url to filter the undesirable links

    Returns:
        (list) a list of urls to be scraped for details of president/secretary
               visits
    """

    urls_to_follow = []
    first_country = "afghanistan"  # when first country appears, terminate loop
    for item in soup.find_all("a"):
        try:  # there is possibility that "a" tag has no "href" inside
            href = item["href"]
            if first_country in href:  # reached locations, thus terminate
                break
            if url_body in href:  # navigate only the desirable urls
                urls_to_follow.append(f"{url_header}{href}")
        except:
            continue
    return urls_to_follow


def crawl_and_scrape_travels(url_list):
    """
    Crawls and scrapes a list of urls and returns a list of details
    on presidential/secretarial travels

    Inputs:
        - url_list (list) a list of urls containing travel info

    Returns:
        (list) a list of lists containing travel info
               each element contains the information of
               [destination country, destination city, description, time]
    """
    out = []
    for url in url_list:
        soup = obtain_soup(url)
        travel_details = soup.find_all("tbody")  # travel info in a tbody tag
        visit = []
        for item in travel_details[0].find_all("td"):
            if len(visit) < 4:  # each visit has 4 attributes
                visit.append(item.text)
            else:  # visit details complete and new visit found
                out.append(visit)
                visit = [item.text]  # new visit found so re-initialize
        time.sleep(21)  # time delay of at least 20 sec between scraping
    return out


def add_year_columns(csv_filename):
    """
    Adds year columns in the scraped data (csv files) in place.

    Inputs:
        - csv_filename (str): filename for csv file containing scraped data

    Returns:
        None
    """
    dataset = pd.read_csv(csv_filename)
    dataset['year'] = dataset['time'].apply(lambda x:
                                            int(re.findall(r'\d{4}', x)[0])
                                            )
    dataset["year_aggregate"] = dataset["year"].apply(lambda x:
                                                      math.ceil(x / 5) * 5
                                                      )
    dataset.to_csv(csv_filename, index=False)


# if __name__ == "__main__":
#     # Call the travel_visit function to obtain the travel information.
#     get_travel_info(HEADER, BODY_PRESIDENT, URL_PRESIDENT, OUT_FILE_PRESIDENT)
#     get_travel_info(HEADER, BODY_SECRETARY, URL_SECRETARY, OUT_FILE_SECRETARY)
#     for csv_file in [OUT_FILE_PRESIDENT, OUT_FILE_SECRETARY]:
#         add_year_columns(csv_file)
