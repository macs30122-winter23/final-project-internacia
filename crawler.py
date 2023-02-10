from bs4 import BeautifulSoup
import requests
import csv
import time

# Hard coded parameters for secretary and president visits
HEADER = "https://history.state.gov"
BODY_PRESIDENT = "/departmenthistory/travels/president/"
BODY_SECRETARY = "/departmenthistory/travels/secretary/"
URL_PRESIDENT = "https://history.state.gov/departmenthistory/travels/president"
URL_SECRETARY = "https://history.state.gov/departmenthistory/travels/secretary"
OUT_FILE_PRESIDENT = "./AmericanPresidentVisit.csv"
OUT_FILE_SECRETARY = "./AmericanSecretaryVisit.csv"


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


if __name__ == "__main__":
    # Call the travel_visit function to obtain the travel information.
    get_travel_info(HEADER, BODY_PRESIDENT, URL_PRESIDENT, OUT_FILE_PRESIDENT)
    # get_travel_info(HEADER, BODY_SECRETARY, URL_SECRETARY, OUT_FILE_SECRETARY)
