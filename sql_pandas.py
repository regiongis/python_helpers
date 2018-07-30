"""
Connections for databases and functions for generation connection 
string for SQL Alchemy and creating Pandas dataframe from SQL.
Requires sqlalchemy and pandas.
"""

import pandas as pd
from sqlalchemy import create_engine

class SqlConnection:
    """
    A connection class for a sql database. Define host, name of database, username, password and port.
    """

    def __init__(self, host, dbname, user, passw, port):
        self.host = host
        self.dbname = dbname
        self.user = user
        self.passw = passw
        self.port = port
        self.con_str = f'postgresql://{self.user}:{self.passw}@{self.host}:{self.port}/{self.dbname}'

    def sql_to_dataframe(self, query):
        """
        Takes a SQL query and return Pandas dataframe
        """

        con = create_engine(self.con_str)
        df = pd.read_sql_query(query,con=con)

        return df
