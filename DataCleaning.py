import re
import pandas as pd
import math


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
                                                      math.ceil(x/5) * 5
                                                      )

if __name__ == "__main__":
    for csv_file in ["AmericanPresidentVisit.csv",
                     "AmericanSecretaryVisit.csv"]:
        add_year_columns(csv_file)