import pandas as pd

def clean_data(filename):
    dataset = pd.read_csv(filename,header=None)
    year = []
    for i in range (dataset.shape[0]):
        info = dataset.iloc[i,3]
        year.append(info[-4:-1]+info[-1])
    year = pd.DataFrame(year)
    dataset.insert(4,"Year",year)
    dataset.to_csv(filename,header=False, index=False)

clean_data("AmericanSecretaryVisit.csv")
clean_data("AmericanPresidentVisit.csv")