import pandas as pd
import math

def clean_data(filename):
    dataset = pd.read_csv(filename,header=None)
    year = []
    for i in range (dataset.shape[0]):
        info = dataset.iloc[i,3]
        year.append(info[-4:-1]+info[-1])
    year = pd.DataFrame(year)
    dataset.insert(4,"Year",year)
    dataset.to_csv(filename,header=False, index=False)

def year_sort(filename):
    dataset = pd.read_csv(filename, header=None)
    sorted_year = []
    for i in range (dataset.shape[0]):
        year_new = dataset.iloc[i,4]
        year_new = math.ceil(year_new/5)*5
        sorted_year.append(year_new)
    sorted_year = pd.DataFrame(sorted_year)
    dataset.insert(5,"Sorted_Year",sorted_year)
    dataset.to_csv(filename,header=False, index=False)

clean_data("AmericanPresidentVisit.csv")
clean_data("AmericanSecretaryVisit.csv")
year_sort("AmericanSecretaryVisit.csv")
year_sort("AmericanPresidentVisit.csv")