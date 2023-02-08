from bs4 import BeautifulSoup
import requests
import csv
import time

def travel_visit(url_header, url_body, url ,FileName):
    '''
    Main function of the crawler.
    Input: 
    url_header: The absolute part of the url.
    url_body: The relative part of the url, helps to navigate the correct url.
    url: The original "root" url that we start scraping.
    FileName: Name of the output file.
    Output:
    A csv file of the visit of US president/secretary.
    Column 1: Country Column 2: City Column 3: Description Column 4: Time
    '''
    #First, obtain the information from the root url. Scrape and parse it with beautifulsoup.
    soup = obtain_soup(url)
    #Obtain a list of urls. Each url contains visit information of one president/secretary.
    url_list = url_finder(soup, url_header, url_body)
    #Scrape the infomation from each url in the url_list, have a list of visit details.
    list_visit = crawler(url_list)
    #Write the list of visit details into a csv file.
    with open (FileName, 'w', encoding='utf-8', newline='') as f:
        write = csv.writer(f, delimiter=",")
        write.writerows(list_visit)
        f.close()

#Auxilirary functions.
def obtain_soup(url):
    '''
    Obtain the parsed result for every url to be scraped.
    Input: url
    Output: The parsed "soup" obtained by BeautifulSoup
    '''
    #Get the details of webpage from a url. Obtain the text.
    request = requests.get(url)
    request = request.text
    #Parse it with beautifulsoup.
    soup = BeautifulSoup(request, "html5lib")
    return(soup)

def url_finder(soup, url_header, url_body):
    '''
    Find the list of url on the visit of each president/secretary.
    Input: soup object, url_header(absolute url), url_body to filter the undesirable.
    Output: A list of urls to be scraped for details of president/secretary visit.
    '''
    #Initialize the list of urls.
    url_list = []
    #Truncate the url finding process. The website contains two types of visit:
    #By president and by location. We only need by president, therefore, when the
    #first location (afghanistan) appears, we break the loop and terminate it.
    truncate = "afghanistan"
    for item in soup.find_all("a"):
        #There is possibility that "a" tag has no "href" inside.
        try:
            href = item["href"]
            #If the truncate version is inside the string of url, break the loop.
            #This implies that we already finish the president/secretary search
            #and proceed in to location.
            if truncate in href:
                break
            #url_body is a filter, this helps us navigate the desirable url to be scraped.
            if url_body in href:
                #Combine the absolute url instead of relative url.
                url_final = url_header+href
                url_list.append(url_final)
        except:
            continue
    return(url_list)

def crawler(url_list):
    '''
    Scrape each url in the list. Put the details of the president/secretary travel into a list of list.
    Input: url_list (A list of urls for all presidents/secretaries)
    Output: A list of list on information each visit.
    Each element contains the information of [destination country, destination city, description, time]
    '''
    #Initialize the list of result.
    list_init = []
    for i in range (len(url_list)):
        soup = obtain_soup(url_list[i])
        #The travel details is stored inside the tbody tag.
        tbody = soup.find_all("tbody")
        #Initialize the visit.
        visit = []
        for item in tbody[0].find_all("td"):
            #Each visit has 4 attributes. If the length of a visit is smaller than 4, that means there is
            #still information to be added to that visit.
            if len(visit) < 4:
                visit.append(item.text)
            else:
                #If its length is equal to 4, we have added all information. Then add it in the result.
                list_init.append(visit)
                #Initialize the visit again and add this information as the first entry of a new visit.
                visit = []
                visit.append(item.text)
        #Just in case there is some protection mechanism forbids us scrape too quickly.
        time.sleep(1)
    return(list_init)

#Hard code the parameter of secretary and president visit.
header = "https://history.state.gov"
body_president = "/departmenthistory/travels/president/"
body_secretary = "/departmenthistory/travels/secretary/"
url_president = "https://history.state.gov/departmenthistory/travels/president"
url_secretary = "https://history.state.gov/departmenthistory/travels/secretary"
file_president = "AmericanPresidentVisit.csv"
file_secretary = "AmericanSecretaryVisit.csv"
#Call the travel_visit function to obtain the travel information.
travel_visit(header, body_president, url_president, file_president)
travel_visit(header, body_secretary, url_secretary, file_secretary)