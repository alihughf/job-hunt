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
        return(f"Script ran on {self.database}: {script}")
    
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

def create_tables(db_client: SQLClient):
    glasses_dim ="""CREATE TABLE glasses_dim (
        GlassID int NOT NULL PRIMARY KEY,
        GlassName varchar(255) NOT NULL
    )"""
    drinks_dim = """CREATE TABLE drinks_dim (
        DrinkID int NOT NULL PRIMARY KEY,
        DrinkName varchar(255) NOT NULL,
        GlassID int NOT NULL,
        FOREIGN KEY (GlassID) REFERENCES glasses_dim(GlassID)
    )"""
    bar_dim ="""CREATE TABLE bar_dim (
        BarID int NOT NULL PRIMARY KEY,
        BarName varchar(255) NOT NULL
    )"""
    price_table = """CREATE TABLE prices (
        DrinkID int NOT NULL,
        BarID int NOT NULL,
        Price real NOT NULL,
        FOREIGN KEY (DrinkID) REFERENCES drinks_dim(DrinkID),
        FOREIGN KEY (BarID) REFERENCES bar_dim(BarID)
        CONSTRAINT PK_prices PRIMARY KEY (BarID,DrinkID)
    )"""
    sales_table ="""CREATE TABLE sales (
        Time real NOT NULL ,
        DrinkID int NOT NULL,
        BarID int NOT NULL,
        FOREIGN KEY (DrinkID) REFERENCES drinks_dim(DrinkID),
        FOREIGN KEY (BarID) REFERENCES bar_dim(BarID)
    )"""
    stock_table = """CREATE TABLE stock (
        BarID int NOT NULL,
        GlassID int NOT NULL,
        Stock int NOT NULL,
        FOREIGN KEY (BarID) REFERENCES bar_dim(BarID),
        FOREIGN KEY (GlassID) REFERENCES glasses_dim(GlassID)
        CONSTRAINT PK_stock PRIMARY KEY (BarID,GlassID)
    )"""

    db_client.build_tables(glasses_dim)
    db_client.build_tables(drinks_dim)
    db_client.build_tables(bar_dim)
    db_client.build_tables(price_table)
    db_client.build_tables(sales_table)
    db_client.build_tables(stock_table)

def create_data_from_csv(

):
    pass

def main():
    cocktail_client = CocktailClient()
    db_client = SQLClient(database="bars.db")
