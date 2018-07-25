"""
Connections for databases and functions for generation connection 
string for SQL Alchemy and creating Pandas dataframe from SQL.

example:
import sys
sys.path.append('/path/to/folder')
import connections as con

con.sql_to_dataframe('DISTRIBUTION', 'select * from proj_belysning.belysning limit 10')
"""

import pandas as pd
from sqlalchemy import create_engine

PRODUCTION = {
    'host': 'xxx',
    'dbname': 'xxx',
    'user': 'xxx',
    'password': 'xxx',
    'port': 5432
}

DISTRIBUTION = {
    'host': 'xxx',
    'dbname': 'xxx',
    'user': 'xxx',
    'password': 'xxx',
    'port': 5432
}

COLLECTOR = {
    'host': 'xxx',
    'dbname': 'xxx',
    'user': 'xxx',
    'password': 'xxx',
    'port': 5432
}

def get_connection_sting(connection):
    """
    Returns connection string for SQL Alchemy engine.
    Following connections are available:
    'PRODUCTION', 
    'DISTRIBUTION',
    'COLLECTOR'
    """

    if connection not in ['PRODUCTION', 'DISTRIBUTION', 'COLLECTOR']:
        raise ValueError("Couldn't not find connection with given name")

    if connection == 'PRODUCTION':
        server = PRODUCTION
    if connection == 'DISTRIBUTION':
        server = DISTRIBUTION
    if connection == 'COLLECTOR':
        server = COLLECTOR

    user =  server.get('user')
    password = server.get('password')
    host = server.get('host')
    port = server.get('port')
    dbname = server.get('dbname')

    con_str = 'postgresql://{0}:{1}@{2}:{3}/{4}'.format(user, password, host, port, dbname)

    return con_str

def sql_to_dataframe(connection, query):
    """
    Takes DB connection and SQL query and return Pandas dataframe
    Following connections are available:
    'PRODUCTION', 
    'DISTRIBUTION',
    'COLLECTOR'
    """

    con_str = get_connection_sting(connection)
    con = create_engine(con_str)
    df = pd.read_sql_query(query,con=con)

    return df