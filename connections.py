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

    def get_connection_sting(self):
        """
        Returns connection string for SQL Alchemy engine.
        """
        con_str = f'postgresql://{self.user}:{self.passw}@{self.host}:{self.port}/{self.dbname}'

        return con_str

    def sql_to_dataframe(self, query):
        """
        Takes a SQL query and return Pandas dataframe
        """

        con_str = self.get_connection_sting()
        con = create_engine(con_str)
        df = pd.read_sql_query(query,con=con)

        return df
