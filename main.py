from crawl_and_scrape import get_travel_info, HEADER, BODY_PRESIDENT, \
    URL_PRESIDENT, OUT_FILE_PRESIDENT, BODY_SECRETARY, URL_SECRETARY, \
    OUT_FILE_SECRETARY, add_year_columns
from diplomatic_exchanges import drop_all_tables, populate_db

# scrape data on presidential visits
get_travel_info(HEADER, BODY_PRESIDENT, URL_PRESIDENT, OUT_FILE_PRESIDENT)
get_travel_info(HEADER, BODY_SECRETARY, URL_SECRETARY, OUT_FILE_SECRETARY)
for csv_file in [OUT_FILE_PRESIDENT, OUT_FILE_SECRETARY]:
    add_year_columns(csv_file)

# # clean/match data, create and populate database
drop_all_tables()
populate_db()