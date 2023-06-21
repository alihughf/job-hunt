# Use this script to write the python needed to complete this task
from apiclient import APIClient
import pandas as pd
import sqlite3




class SQLClient():
    def __init__(
            self,
            database=None
    ):
        self.database = database

    def build_tables(
            self,
            script
    ):
        con = sqlite3.connect(self.database)
        cur = con.cursor()
        cur.execute(script)
        cur.close()
        con.close()
        return(f"Script ran on bars.db: {script}")
    
    def insert_df(
            self,
            df,
            table_name
    ):
        con = sqlite3.connect(self.database)
        df.to_sql(table_name, con, if_exists='append')
        return(None)

    def send_query(
            self,
            query
    ):
        con = sqlite3.connect(self.database)
        cur = con.cursor()
        res = cur.execute(query)
        cur.close()
        con.close()
        return(res.fetchall())
    
    def query_to_df(
            self,
            query
    ):
        con = sqlite3.connect(self.database)
        df = pd.read_sql(query, con)
        return(df)

class CocktailClient(APIClient):
    
    def list_all_cocktails(self, letter):
        url = f"http://www.thecocktaildb.com/api/json/v1/1/search.php?f=a"
        params = {"f": letter}
        return(self.get(url,params))




