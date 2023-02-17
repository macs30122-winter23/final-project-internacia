import csv
import sys

import jellyfish
import networkx as nx
import pandas as pd
import numpy as np
import sqlite3
import matplotlib.pyplot as plt
import warnings
from sklearn.preprocessing import MinMaxScaler

warnings.filterwarnings("ignore")
import os

DATA_FOLDER = './data/'
DIPLOMATIC_DATA_FNAME = 'Diplomatic_Exchange_2006v1.csv'
DATABASE_NAME = "diplomatic.db"
TABLE_NAME = "diplomatic_exchanges"
PRESIDENT_VISITS_FNAME = "AmericanPresidentVisit.csv"
COW_COUNTRY_CODES_FNAME = "COW-country-codes.csv"
ECONOMIC_DATA_FNAME = "PennWorldTable.csv"

with open(f"{DATA_FOLDER}{COW_COUNTRY_CODES_FNAME}", 'r') as f:
    COUNTRY_CODES_DICT = {row['StateNme']: int(row['CCode'])
                          for row in csv.DictReader(f)}


def dump_dataframe_to_db(conn, df, name):
    """
    Dumps dataframe to already existing database,
    If the table already exists prints a warning.

    Inputs:
        conn (sqlite3.Connection) connection to database
        df (pandas.DataFrame) dataframe
        name (str) name of the table

    Returns: None
    """
    cur = conn.cursor()
    response = cur.execute(f"""SELECT name FROM sqlite_master 
                              WHERE type='table' AND name='{name}'"""
                           ).fetchall()
    cur.close()
    if not response:  # if table already in db, then skip
        df.to_sql(name=name, con=conn)
    else:
        print(f"Table {name} already exists in {DATABASE_NAME} \
        | should have columns {df.columns} and size {df.shape}")
    cur.close()


# def country_codes_map():
#     """
#     Map country names to country codes
#
#     Inputs: None
#
#     Returns:
#          (dict) mapping from country names to country codes
#                 e.g. {"United States of America": 2}
#     """
#     # country codes to country name mapping
#     with open(f"{DATA_FOLDER}{COW_COUNTRY_CODES_FNAME}", 'r') as f:
#         country_codes_dict = {row['StateNme']: int(row['CCode'])
#                               for row in csv.DictReader(f)}
#     return country_codes_dict


def normalize_dataframe(df):
    """
    Normalize dataframe using a MinMax scaler

    Inputs:
        - df (pandas.DataFrame) dataframe

    Returns:
        (pandas.DataFrame) the normalized dataframe
    """
    columns = df.columns
    scaler = MinMaxScaler()
    scaler.fit(df.values)
    return pd.DataFrame(scaler.transform(df.values), columns=columns)


def compute_centrality_measures(conn, year, centrality_table_name, to_csv):
    """
    Compute centrality measures for a given year.

    example query for year 2005:
        SELECT *  FROM diplomatic_exchanges de
        WHERE DE=1 AND DR_at_1=3 AND year=2005 AND DR_at_2 != 9

    Inputs:
        - conn (sqlite3.Connection) connection to database
        - year (int) year to compute centralities
        - centrality_table_name (str) name to store the table in the database
        - to_csv (bool) also dump tables to csv files on disk

    Returns: None
    """

    q = f""" SELECT *  FROM {TABLE_NAME} de 
             WHERE DE=1 AND DR_at_1 = 3 AND year ={year} AND DR_at_2 != 9
         """

    diplomatic_exchanges_per_year = pd.read_sql(q, conn)

    G_per_year = nx.from_pandas_edgelist(
        diplomatic_exchanges_per_year[['ccode1', 'ccode2', 'DR_at_2']],
        source='ccode1', target='ccode2',
        edge_attr='DR_at_2',
        create_using=nx.DiGraph)

    # compute centrality measures
    page_rank_scores_dict = nx.pagerank(G_per_year, weight='DR_at_2')
    betweenness_dict = nx.betweenness_centrality(G_per_year,
                                                 weight='DR_at_2')
    closeness_dict = nx.closeness_centrality(G_per_year)
    degree_dict = nx.degree(G_per_year, weight='DR_at_2')
    in_degree_dict = nx.in_degree_centrality(G_per_year)
    out_degree_dict = nx.out_degree_centrality(G_per_year)
    nodes = G_per_year.nodes

    df_centralities = pd.DataFrame({'pagerank': [page_rank_scores_dict[i]
                                                 for i in nodes],
                                    'betweenness': [betweenness_dict[i]
                                                    for i in nodes],
                                    'closeness': [closeness_dict[i]
                                                  for i in nodes],
                                    'degree': [degree_dict[i]
                                               for i in nodes],
                                    'in_degree': [in_degree_dict[i]
                                                  for i in nodes],
                                    'out_degree': [out_degree_dict[i]
                                                   for i in nodes]
                                    })
    df_centralities = normalize_dataframe(df_centralities)
    df_centralities['node_id'] = list(nodes)
    df_centralities['year'] = [year] * len(nodes)

    if to_csv:
        if not os.path.exists('./data/centrality_measures'):
            os.mkdir('./data/centrality_measures')
        df_centralities.to_csv(
            f'./data/centrality_measures/centrality_{year}.csv')

    dump_dataframe_to_db(conn, df_centralities, name=centrality_table_name)


def create_all_centrality_measure_tables(conn, diplomatic_exchanges, to_csv):
    """
    Creates a table containing centrality measures for all years.

    Computes intermediary tables for each year and creates a union table
    containing centrality measures for all years (normalized for each year).
    Note that the intermediary tables are finally dropped from the database
    but can be saved as csv files.

    Inputs:
        - conn (sqlite3.Connection) connection to database
        - diplomatic_exchanges (pandas.DataFrame) diplomatic data
        - to_csv (bool) if True dump resulting dataframe to csv on disk

    Returns: None
    """
    years = sorted(set(diplomatic_exchanges["year"].values))
    centrality_table_names = []
    for year in years:
        if year in (1950, 1955, 1960, 1965):  # only 0 and 9 relationships
            continue
        centrality_table_name = f"centrality_{year}"
        centrality_table_names.append(centrality_table_name)
        compute_centrality_measures(conn, year, centrality_table_name, to_csv)

    q = " UNION ".join([f"SELECT * FROM {table}"
                        for table in centrality_table_names])

    all_centralities = pd.read_sql(q, conn).drop('index', axis=1)
    dump_dataframe_to_db(conn, all_centralities, name="all_centralities")

    if to_csv:
        all_centralities.to_csv(
            './data/centrality_measures/all_centralities.csv')
    drop_tables(conn, centrality_table_names)  # drop intermediary tables


def add_presidential_visits(conn):
    """
    Add presidential visits to database.

    Inputs:
        - conn (sqlite3.Connection) connection to database

    Returns: None
    """
    president_visits = pd.read_csv(f"{DATA_FOLDER}{PRESIDENT_VISITS_FNAME}",
                                   names=["destination country",
                                          "destination city",
                                          "description", "time", "year",
                                          "year_aggregate"])

    scraped_countries = set(president_visits['destination country'].values)
    cow_countries = set(pd.read_csv(
        f'{DATA_FOLDER}/{COW_COUNTRY_CODES_FNAME}')['StateNme'].values)

    countries_to_codes_dict = {scraped_country:
                                   COUNTRY_CODES_DICT[scraped_country]
                               for cow_country in cow_countries
                               for scraped_country in scraped_countries
                               if jellyfish.jaro_winkler_similarity(
            scraped_country, cow_country) == 1
                               }
    corrected_mismatches_dict = {'Bosnia-Herzegovina': 'Bosnia and Herzegovina',
                                 'Brunei Darussalam': 'Brunei',
                                 'China, People’s Republic of': 'China',
                                 'Burma': 'Myanmar',
                                 'Germany, Federal Republic of': 'Germany',
                                 'Korea, Republic of': 'Korea',
                                 'Korea, South': 'South Korea',
                                 'Macedonia, Former Yugoslav Republic of': 'Macedonia',
                                 'Republic of China': 'Taiwan',
                                 'Serbia-Montenegro (Kosovo)': 'Kosovo',
                                 'U.S.S.R.': 'Russia',
                                 'Vatican City': 'Papal States',
                                 'Yugoslavia (Kosovo)': 'Kosovo'
                                 }
    for k in corrected_mismatches_dict:
        countries_to_codes_dict[k] \
            = COUNTRY_CODES_DICT[corrected_mismatches_dict[k]]

    president_visits['ccode'] = president_visits['destination country'].replace(
        countries_to_codes_dict)

    dump_dataframe_to_db(conn, president_visits, "president_visits")


def populate_db(to_csv=True):
    """
    Populate database with diplomatic exchange data.

    Inputs:
        - to_csv (bool) if True also save tables as csv files

    Returns: None
    """
    conn = sqlite3.connect(DATABASE_NAME)

    diplomatic_exchanges = pd.read_csv(f"{DATA_FOLDER}{DIPLOMATIC_DATA_FNAME}")
    dump_dataframe_to_db(conn, diplomatic_exchanges, name=TABLE_NAME)

    create_all_centrality_measure_tables(conn, diplomatic_exchanges, to_csv)

    add_presidential_visits(conn)

    economic_data = pd.read_csv(f"{DATA_FOLDER}{ECONOMIC_DATA_FNAME}")
    dump_dataframe_to_db(conn, economic_data, 'economic_data')

    conn.close()


def drop_tables(conn, table_names):
    """
    Drop tables from database.

    Inputs:
        - conn (sqlite3.Connection) connection to database
        - table_names (list) list of table names to drop

    Returns: None
    """
    cur = conn.cursor()
    for table in table_names:
        q = f"DROP TABLE {table}"
        cur.execute(q)
    cur.close()


def drop_all_tables():
    """
    Drop all tables from database.

    Inputs: None

    Returns: None
    """
    conn = sqlite3.connect(DATABASE_NAME)
    cur = conn.cursor()
    q = "select name from sqlite_master WHERE type='table';"
    table_names = map(lambda x: x[0], cur.execute(q).fetchall())

    for table in table_names:
        q = f"DROP TABLE {table}"
        cur.execute(q)
    cur.close()
    conn.close()


if __name__ == "__main__":
    populate_db()
