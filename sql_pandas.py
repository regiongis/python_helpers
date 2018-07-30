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

    :host: The host of the SQL database.
    :dbname: The name of the database.
    :user: The username of the user that connects to the database.
    :passw: The password of the user.
    :port: The port of the database.
    """

    def __init__(self, host, dbname, user, passw, port):

        self.con_str = f'postgresql://{user}:{passw}@{host}:{port}/{dbname}'
        self.engine = create_engine(self.con_str)

    def sql_to_dataframe(self, query):
        """
        Takes a SQL query and return Pandas dataframe.

        :query: A SQL search query.
        :returns: A pandas dataframe object.
        """

        df = pd.read_sql_query(query,con=self.engine)
        return df

    def dataframe_to_sql(self, df, name, schema):
        """
        Imports a dataframe into a SQL database.

        :df: Dataframe that should be imported.
        :name: Name of table in database.
        :schema: Name of schema in database.
        """
        
        df.to_sql(name, self.engine, schema=schema, index=False)
