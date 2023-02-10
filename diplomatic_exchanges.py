import csv
import sys
import networkx as nx
import pandas as pd
import numpy as np
import sqlite3
import matplotlib.pyplot as plt
import warnings

warnings.filterwarnings("ignore")
import os

def diplomatic_data_to_database():
    df = pd.read_csv('./data/Diplomatic_Exchange_2006v1.csv')

    # create a db to work with
    conn = sqlite3.connect('diplomatic.db')

    cur = conn.cursor()
    response = cur.execute("""SELECT name FROM sqlite_master 
                              WHERE type='table' AND name='diplomatic_exchages'"""
                           ).fetchall()
    if not response:  # if table already in db, then skip
        # dump diplomatic exchange dataset to the database
        df.to_sql(name='diplomatic_exchages', con=conn)

    years = sorted(set(df.year.values))

    # country codes to country name mapping
    with open('./data/COW-country-codes.csv', 'r') as f:
        country_codes_dict = {int(row['CCode']): row['StateNme']
                              for row in csv.DictReader(f)}

    # compute centrality measures dump into csv and database
    for year in years:
        if year in (1950, 1955, 1960, 1965):  # only 0 and 9 relationships for these
            continue
        # weighted graph according to international relation
        q = """
        SELECT *  FROM diplomatic_exchages de
        WHERE DE=1 AND DR_at_1 = 3 AND year ={} AND DR_at_2 != 9
        """.format(year)
        df_year = pd.read_sql(q, conn)
        G_year = nx.from_pandas_edgelist(df_year[['ccode1', 'ccode2', 'DR_at_2']],
                                         source='ccode1', target='ccode2',
                                         edge_attr='DR_at_2',
                                         create_using=nx.DiGraph)

        # compute centrality measures
        page_rank_scores_dict = nx.pagerank(G_year, weight='DR_at_2')
        betweenness_dict = nx.betweenness_centrality(G_year, weight='DR_at_2')
        closeness_dict = nx.closeness_centrality(G_year)
        degree_dict = nx.degree(G_year, weight='DR_at_2')
        in_degree_dict = nx.in_degree_centrality(G_year)
        out_degree_dict = nx.out_degree_centrality(G_year)
        nodes = G_year.nodes

        # write centrality measures to csv per year
        df_out = pd.DataFrame()
        df_out['node_id'] = list(nodes)
        df_out['year'] = [year] * len(nodes)
        df_out['pagerank'] = [page_rank_scores_dict[i] for i in nodes]
        df_out['betweenness'] = [page_rank_scores_dict[i] for i in nodes]
        df_out['closeness'] = [closeness_dict[i] for i in nodes]
        df_out['degree'] = [degree_dict[i] for i in nodes]
        df_out['in_degree'] = [in_degree_dict[i] for i in nodes]
        df_out['out_degree'] = [out_degree_dict[i] for i in nodes]
        if not os.path.exists('./data/centrality_measures'):
            os.mkdir('./data/centrality_measures')
        df_out.to_csv(
            f'./data/centrality_measures/graph_centrality_scores_{year}.csv')

        response = cur.execute("""SELECT name FROM sqlite_master 
                                  WHERE type='table' AND name='centrality_{}'"""
                               .format(year)
                              ).fetchall()
        if not response:
            # dump centrality measure per year tables to sql
            df_out.to_sql(name=f'centrality_{year}', con=conn)

    # join per year tables
    response = cur.execute("""SELECT name FROM sqlite_master
                              WHERE name LIKE 'centra%'
                              """).fetchall()
    q = " union ".join([f"SELECT * FROM {table[0]}" for table in response])
    df_centralities = pd.read_sql(q, conn).drop('index', axis=1)

    # dump joined tables to database
    response = cur.execute("""SELECT name FROM sqlite_master 
                                  WHERE type='table' AND 
                                  name='all_centralities'"""
                              ).fetchall()
    if not response:
        df_centralities.to_sql(name='all_centralities', con=conn)

def drop_all_tables():
    conn = sqlite3.connect('./diplomatic.db')
    cur = conn.cursor()
    q = "select name from sqlite_master WHERE type='table';"
    table_names = map(lambda x: x[0], cur.execute(q).fetchall())

    for table in table_names:
        q = f"DROP TABLE {table}"
        cur.execute(q)
    # return list(table_names)