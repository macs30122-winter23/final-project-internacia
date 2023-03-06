# Final Project - MACS 30122 Computer Science with Social Science Applications
- Professor: Prof. Sabrina Nardin
- Team-name: Internacia
- Members:
  - Loizos Bitsikokos: bitsikokos@uchicago.edu
  - Roberto Rondo Garces: robertor@uchicago.edu
  - Yutao He: yutaohe@uchicago.edu

**Goal** : Define a relational measure of a country's status using data of diplomatic exchange. 

**Research Question**: Does a country's status measured by network centrality measures predict the likelihood of a US president visiting it?

**Hypothesis**: A network-based measure of status correlates better with foreign travels made by heads of states compared to material attributes.

## Summary
Status is a core concept in International relations. 
Traditional approaches measure status by state attributes such as economic or military power.
We propose an alternative approach to measuring status that leverages networks of diplomatic exchange.
Weber defined status as “an effective claim to social esteem in terms of positive or negative privileges”.
Since each country has limited diplomatic resources, it must prioritize where to send its diplomats.
We use the PageRank Algorithm and other centrality measures, assuming that each diplomat sent 
by country $j$ to country $i$ is a vote for the importance of $i$. 
Our hypothesis is that a relational measure of status correlates better with foreign travels made by heads of states compared to material and other attributes.
We use diplomatic exchange data to build a temporal diplomatic network between countries and subsequently compute various centrality measures on this network.
We further scrape data on US presidential visits to foreign countries and then link them to the computed centrality measures as well as tables of economic and power data.
We aggregate all our datasets in an sqlite database and answer our research question and evaluate our hypothesis using logistic regression model. 
Finally, we provide a visualization of the diplomatic network. 

## Data-sources
[Correlates of War Datasets](https://correlatesofwar.org/data-sets/cow-war/):
- [Diplomatic exchange data](https://correlatesofwar.org/wp-content/uploads/Diplomatic_Exchange_2006v1.csv)
- [Power data](https://correlatesofwar.org/wp-content/uploads/NMC_Documentation-6.0.zip)

[Economic data - Pennworld table](https://dataverse.nl/api/access/datafile/354098)

[Scraped data - Presidential visits](https://history.state.gov/departmenthistory/travels/president)

## Library requirements
All of our conda environment requirements can be found in the `cond_env.yaml` file.
We briefly list our dependencies:
- `matplotlib=3.6.2=py38haa95532_0`
- `pandas=1.5.2=py38hf11a4ad_0`
- `numpy=1.23.5=py38h3b20f71_0`
- `sqlite=3.40.1=h2bbff1b_0`
- `requests=2.28.1=py38haa95532_0`
- `bs4=4.11.1=hd3eb1b0_0`
- `networkx=2.8.4=py38haa95532_0`
- `pyvis=0.3.1=pyhd8ed1ab_0`
- `seaborn=0.12.2=py38haa95532_0`

## Code navigation
Most of our project (data acquisition, crawling and scraping, cleaning, processing, merging and database build) can be run by simply running our `main.py`.
For example:
```
>> python main.py
```
This script first calls our crawler which crawls websites downloading presidential visit data (crawler can be found in `crawl_and_scrape.py`). 

It also downloads the necessary pre-existing datasets. All the data are stored in a `./data/` folder (see `diplomatic_exchanges.py`).

Note that `diplomatic_exchanges.py` not only creates and populates an SQLite database (`diplomatic.db`) with our data but also provides functions that run queries on the database (neccessary for the modelling part of the project).

We also provide two jupyter notebooks:
- Network visualization
	- The `exploratory_network_analysis.ipynb` provides code that examines the diplomatic network. It also outputs gephi files for our final network visualization.
-  Modelling
	- The `regression_analysis.ipynb` contains code that gets linked data on presidential visits across time from the database and  runs multiple logistic regression models to evaluate our hypothesis.

# Tasks

- Data cleaning: Loizos, Roberto (`diplomatic_exchanges.py`)
- Data scraping: Loizos, Yutao (`crawl_and_scrape.py`)
- Modeling: Loizos, Yutao, Roberto (`regression_analysis.ipynb`, `exploratory_network_analysis.ipynb`)

# Slides
A link to our slides can be found here: [In-calss Presentation](https://docs.google.com/presentation/d/1-jXy8YLsqMQQBUWH2TPsABMJPtoY0TBQ8QDnlLAUPsw/edit?usp=sharing)

# Video
A link to our video can be found here
