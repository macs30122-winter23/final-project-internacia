import jellyfish
import networkx as nx
import pandas as pd
import sqlite3
import warnings

from sklearn.preprocessing import MinMaxScaler

from crawl_and_scrape import DATA_FOLDER, COUNTRIES_TO_CODES_DICT, \
    PRESIDENT_VISITS_FNAME, ECONOMIC_DATA_FNAME, DIPLOMATIC_DATA_FNAME, \
    POWER_DATA_FNAME

warnings.filterwarnings("ignore")
import os

# SQl table names and database name
DIPLOMATIC_DATA_TABLE_NAME = "diplomatic_exchanges"
CENTRALITIES_TABLE_NAME = "all_centralities"
ECONOMIC_DATA_TABLE_NAME = "economic_data"
DATABASE_NAME = "diplomatic.db"


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


def compute_centrality_measures(G_per_year, year):
    """
    Computes centrality measures for a given year-graph

    Inputs:
        - G_per_year (nx.Digraph): the graph of diplomatic connections for
                                   the given year
        - year (int): the corresponding year

    Returns:
        (pandas.DataFrame) a pandas Dataframe containing the centrality measures
                           node id and year
    """
    page_rank_scores_dict = nx.pagerank(G_per_year, weight='DR_at_2')
    katz_dict = nx.katz_centrality_numpy(G_per_year, weight='DR_at_2')
    eigen_dict = nx.eigenvector_centrality(G_per_year.to_undirected(),
                                           weight='DR_at_2')
    betweenness_dict = nx.betweenness_centrality(G_per_year.to_undirected(),
                                                 weight='DR_at_2')
    closeness_dict = nx.closeness_centrality(G_per_year.to_undirected(),
                                             distance='DR_at_2')

    degree_dict = nx.degree_centrality(G_per_year.to_undirected())
    in_degree_dict = nx.in_degree_centrality(G_per_year)
    out_degree_dict = nx.out_degree_centrality(G_per_year)
    nodes = G_per_year.nodes

    df_centralities = pd.DataFrame({'pagerank': [page_rank_scores_dict[i]
                                                 for i in nodes],
                                    'eigenvector': [eigen_dict[i]
                                                    for i in nodes],
                                    'katz': [katz_dict[i] for i in nodes],
                                    'betweenness': [betweenness_dict[i]
                                                    for i in nodes],
                                    'closeness': [closeness_dict[i]
                                                  for i in nodes],
                                    'degree': [degree_dict[i] for i in nodes],
                                    'in_degree': [in_degree_dict[i]
                                                  for i in nodes],
                                    'out_degree': [out_degree_dict[i]
                                                   for i in nodes]
                                    })
    df_centralities = normalize_dataframe(df_centralities)
    df_centralities['node_id'] = list(nodes)
    df_centralities['year'] = [year] * len(nodes)
    return df_centralities


def add_centrality_measures_to_db_for_year(conn, year, centrality_table_name,
                                           to_csv):
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

    G_per_year = get_diplomatic_graph(conn, year)

    df_centralities = compute_centrality_measures(G_per_year, year)

    if to_csv:
        if not os.path.exists(f'{DATA_FOLDER}centrality_measures'):
            os.mkdir(f'{DATA_FOLDER}centrality_measures')
        df_centralities.to_csv(
            f'{DATA_FOLDER}centrality_measures/centrality_{year}.csv')

    dump_dataframe_to_db(conn, df_centralities, name=centrality_table_name)


def get_centrality_measures(year):
    """
    Once in db get for a year
    """
    conn = sqlite3.connect(DATABASE_NAME)
    q = f'SELECT * FROM all_centralities ac WHERE ac."year"={year};'
    cur = conn.cursor()
    df = pd.read_sql(q, conn)
    cur.close()
    conn.close()
    return df


def get_diplomatic_graph(conn, year):
    """
    Once in db get a graph object
    """
    q = f""" SELECT *  FROM {DIPLOMATIC_DATA_TABLE_NAME} de 
             WHERE DE=1 AND DR_at_1 = 3 AND year ={year} AND DR_at_2 != 9
         """

    diplomatic_exchanges_per_year = pd.read_sql(q, conn)

    G_per_year = nx.from_pandas_edgelist(
        diplomatic_exchanges_per_year[['ccode1', 'ccode2', 'DR_at_2']],
        source='ccode1', target='ccode2',
        edge_attr='DR_at_2',
        create_using=nx.DiGraph)
    return G_per_year


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
        add_centrality_measures_to_db_for_year(conn, year,
                                               centrality_table_name, to_csv)

    q = " UNION ".join([f"SELECT * FROM {table}"
                        for table in centrality_table_names])

    all_centralities = pd.read_sql(q, conn).drop('index', axis=1)
    dump_dataframe_to_db(conn, all_centralities, name=CENTRALITIES_TABLE_NAME)

    if to_csv:
        all_centralities.to_csv(
            f'{DATA_FOLDER}centrality_measures/{CENTRALITIES_TABLE_NAME}.csv')
    drop_tables(conn, centrality_table_names)  # drop intermediary tables


def match_countries(target_countries, known_mismatches_corrected):
    """
    Match countries and known country codes.

    Inputs:
        - target_countries (set) set of countries to be matched with codes
        - known_mismatches_corrected (dict) pre-known mismatches
                                            mapped to correct country names

    Returns
        (dict) mapping from countries to coutnry codes
    """
    known_countries = set(COUNTRIES_TO_CODES_DICT.keys())
    countries_to_codes_dict = {target_country:
                                   COUNTRIES_TO_CODES_DICT[target_country]
                               for known_country in known_countries
                               for target_country in target_countries
                               if jellyfish.jaro_winkler_similarity(
            target_country, known_country) == 1
                               }

    for k in known_mismatches_corrected:
        countries_to_codes_dict[k] \
            = COUNTRIES_TO_CODES_DICT[known_mismatches_corrected[k]]

    return countries_to_codes_dict


def add_presidential_visits(conn):
    """
    Add presidential visits to database matching countries with country codes

    Inputs:
        - conn (sqlite3.Connection) connection to database

    Returns: None
    """
    president_visits = pd.read_csv(f"{DATA_FOLDER}{PRESIDENT_VISITS_FNAME}")

    scraped_countries = set(president_visits['destination country'].values)

    known_corrected_mismatches = {
        'Bosnia-Herzegovina': 'Bosnia and Herzegovina',
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

    countries_to_codes_dict = match_countries(scraped_countries,
                                              known_corrected_mismatches)

    president_visits['ccode'] = president_visits['destination country'].replace(
        countries_to_codes_dict)

    dump_dataframe_to_db(conn, president_visits, "president_visits")


def add_economic_data(conn):
    """
    Add economic data on database matching countries with country codes.

    Inputs:
        - conn (sqlite3.Connection) database connection

    Returns: None
    """
    economic_data = pd.read_stata(f"{DATA_FOLDER}{ECONOMIC_DATA_FNAME}")

    economic_data_countries = set(economic_data['country'].values)

    known_corrected_mismatches = {'Antigua and Barbuda': 'Antigua & Barbuda',
                                  'Bolivia (Plurinational State of)': 'Bolivia',
                                  'Brunei Darussalam': 'Brunei',
                                  'D.R. of the Congo': 'Democratic Republic of the Congo',
                                  'Eswatini': 'Swaziland',
                                  'Iran (Islamic Republic of)': 'Iran',
                                  "Lao People's DR": 'Laos',
                                  'North Macedonia': 'Macedonia',
                                  'Republic of Korea': 'Korea',
                                  'Republic of Moldova': 'Moldova',
                                  'Russian Federation': 'Russia',
                                  'Syrian Arab Republic': 'Syria',
                                  'U.R. of Tanzania: Mainland': 'Tanzania',
                                  'United States': 'United States of America',
                                  'Venezuela (Bolivarian Republic of)': 'Venezuela',
                                  'Viet Nam': 'Vietnam'}

    countries_to_codes_dict = match_countries(economic_data_countries,
                                              known_corrected_mismatches)
    economic_data['ccode'] = economic_data['country'].replace(
        countries_to_codes_dict)

    dump_dataframe_to_db(conn, economic_data, ECONOMIC_DATA_TABLE_NAME)


def set_foreign_keys(conn):
    """
    Set foreign key relationships between tables in db
    """
    q = """
        -- connect centrality measures with diplomatic exchanges (all_centralities -> diplomatic_exchanges)
        CREATE TABLE all_centralities_new(
        "index" INTEGER PRIMARY KEY,
        pagerank REAL,
        katz REAL,
        eigenvector REAL,
        betweenness REAL,
        closeness REAL,
        "degree" REAL,
        in_degree REAL,
        out_degree REAL,
        node_id INTEGER,
        "year" INTEGER,
        FOREIGN KEY("year") REFERENCES diplomatic_exchanges ("year"),
        FOREIGN KEY(node_id) REFERENCES diplomatic_exchanges (ccode1) ON UPDATE CASCADE ON DELETE CASCADE,
        FOREIGN KEY(node_id) REFERENCES diplomatic_exchanges (ccode2) ON UPDATE CASCADE ON DELETE CASCADE
        );

        INSERT INTO all_centralities_new SELECT * FROM all_centralities;
        DROP TABLE all_centralities;
        ALTER TABLE all_centralities_new RENAME TO all_centralities;
        SELECT * from all_centralities ac ;

        -- connect president visits with centrality table (president_visits -> all_centralities)
        create table president_visits_new(
         "index" INTEGER PRIMARY KEY,
            "destination country" TEXT,
            "destination city" TEXT,
            description TEXT,
            "time" TEXT, 
            "year" INTEGER,
            year_aggregate INTEGER,
            ccode INTEGER,
            FOREIGN KEY(year_aggregate) REFERENCES all_centralities("year"),
            FOREIGN KEY(ccode) REFERENCES all_centralities(node_id) ON UPDATE CASCADE ON DELETE CASCADE
        );


        INSERT INTO president_visits_new SELECT * from president_visits;

        DROP TABLE president_visits;
        ALTER TABLE president_visits_new RENAME TO president_visits;

        -- connect econ data with president visits (economic_data -> president_visits)
        CREATE TABLE economic_data_new(
        "index" INTEGER, countrycode TEXT, country TEXT, currency_unit TEXT, 
        "year" INTEGER, rgdpe REAL, rgdpo REAL, pop REAL, 
        emp REAL, avh TEXT, hc REAL, ccon REAL, cda REAL, cgdpe REAL, cgdpo REAL,
        cn REAL, ck REAL, ctfp REAL, cwtfp REAL, rgdpna REAL, rconna REAL,
        rdana REAL, rnna REAL, rkna REAL, rtfpna REAL, rwtfpna REAL, labsh REAL,
        irr REAL, delta REAL, xr REAL, pl_con REAL, pl_da REAL, pl_gdpo REAL,
        i_cig TEXT, i_xm TEXT, i_xr TEXT, i_outlier TEXT, i_irr TEXT,
        cor_exp REAL, statcap REAL, csh_c REAL, csh_i REAL, csh_g REAL, 
        csh_x REAL,  csh_m REAL, csh_r REAL, pl_c REAL, pl_i REAL, 
        pl_g REAL, pl_x REAL, pl_m REAL, pl_n REAL, pl_k REAL, ccode TEXT,
        FOREIGN KEY(ccode) REFERENCES president_visits("ccode")
        );

        INSERT INTO economic_data_new SELECT * FROM economic_data ;
        DROP TABLE economic_data;
        ALTER TABLE economic_data_new RENAME TO economic_data;
        """
    cur = conn.cursor()
    cur.executescript(q)
    cur.close()


def populate_db(to_csv=True):
    """
    Populate database with diplomatic exchange data.

    Inputs:
        - to_csv (bool) if True also save tables as csv files

    Returns: None
    """
    conn = sqlite3.connect(DATABASE_NAME)

    diplomatic_exchanges = pd.read_csv(f"{DATA_FOLDER}{DIPLOMATIC_DATA_FNAME}")
    dump_dataframe_to_db(conn, diplomatic_exchanges,
                         name=DIPLOMATIC_DATA_TABLE_NAME)

    power_data = pd.read_csv(f"{DATA_FOLDER}{POWER_DATA_FNAME}")
    dump_dataframe_to_db(conn, power_data, 'power_data')

    create_all_centrality_measure_tables(conn, diplomatic_exchanges, to_csv)

    add_presidential_visits(conn)

    add_economic_data(conn)

    set_foreign_keys(conn)

    conn.commit()
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


def get_data_for_regression(conn, year):
    """
    Runs query that aggregates all matched data on presidential visits
    for a given year.

    Inputs:
        - conn (sqlite3.Connection) connection to database
        - year (int) the given year

    Returns:
        (pandas.DataFrame) the dataframe containing
                           all the matched info on visits
    """
    q = f"""
        -- connect president visits with centralities and econ measures
        select * from all_centralities ac 
        left join president_visits pv 
        on ac.node_id == pv.ccode and ac.year == pv.year_aggregate 
        left join economic_data ed
        on ed.ccode == ac.node_id and ed.year=ac.year
        left join power_data 
        on power_data.ccode == ac.node_id and power_data."year" == ac."year" 
        where ac."year" == {year} 
        GROUP by ac.node_id;
    """
    data = pd.read_sql(q, conn)
    return data
