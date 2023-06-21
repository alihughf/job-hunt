# Use this script to write the python needed to complete this task
from apiclient import APIClient
import pandas as pd
import sqlite3
import time

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
        url = f"http://www.thecocktaildb.com/api/json/v1/1/search.php?"
        params = {"f": letter}
        time.sleep(1)
        return(self.get(url,params))
    
    def get_info_for_cocktail_name(self, cocktail_name):
        url = "http://www.thecocktaildb.com/api/json/v1/1/search.php?"
        params = {"s": cocktail_name}
        time.sleep(1)
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

def return_glass(cocktail_client, drink_name):
    res = cocktail_client.get_info_for_cocktail_name(drink_name).json()
    return (res['drinks'][0]['strGlass'])

def main():
    cocktail_client = CocktailClient()
    db_client = SQLClient(database="bars.db")
    create_tables(db_client=db_client)

    bar_data = pd.read_csv("data/bar_data.csv")
    bar_data['glass_type'] = bar_data['glass_type'].replace('coper mug','copper mug')
    london = pd.read_csv("data/london_transactions.csv.gz", compression="gzip", sep=r"\t", names=['datetime','drink','price'], index_col=0)
    budapest = pd.read_csv("data/budapest.csv.gz", compression="gzip",names=['datetime','drink','price'], index_col=0, header=0)
    ny = pd.read_csv("data/ny.csv.gz", compression="gzip",names=['datetime','drink','price'], index_col=0, header=0)

    london['datetime'] = london['datetime'].apply(pd.to_datetime)
    london['BarID'] = 1
    budapest['datetime'] = budapest['datetime'].apply(pd.to_datetime)
    budapest['BarID'] = 2
    ny['datetime'] = ny['datetime'].apply(pd.to_datetime)
    ny['BarID'] = 3

    #Creating glasses dim table
    glasses = bar_data.groupby("glass_type").max().reset_index()
    glasses['ID'] = glasses.index
    glasses.rename(columns = {'ID': 'GlassID', 'glass_type': 'GlassName'}, inplace=True)
    db_client.insert_df(glasses[['GlassID','GlassName']], "glasses_dim")

    #creating drinks table
    all_drinks = pd.DataFrame(set(london['drink'].unique()) | set(budapest['drink'].unique()) | set(ny['drink'].unique()), columns=["DrinkName"])
    all_drinks['DrinkID'] = all_drinks.index
    all_drinks["Glass"] = all_drinks.apply(lambda row: return_glass(cocktail_client, row['DrinkName']), axis=1)
    all_drinks['Glass'] = all_drinks['Glass'].apply(lambda row: row.lower())
    drinks_glass = pd.merge(all_drinks, glasses[['GlassID','GlassName']], left_on='Glass', right_on='GlassName')
    db_client.insert_df(drinks_glass[['DrinkID','DrinkName','GlassID']])

    #creating bar dim table
    bars = pd.DataFrame({"BarID": [1,2,3],"BarName": ["london","budapest","new york"]})
    db_client.insert_df(bars, "bar_dim")

    #creating price table
    london_price = london[['drink','price','BarID']].groupby("drink").first()
    london_price.reset_index(inplace=True)

    budapest_price = budapest[['drink','price','BarID']].groupby("drink").first()
    budapest_price.reset_index(inplace= True)

    ny_price = ny[['drink','price','BarID']].groupby("drink").first()
    ny_price.reset_index(inplace=True)

    price_frames = [london_price, budapest_price, ny_price]
    all_prices = pd.concat(price_frames)
    prices_drinks = pd.merge(all_prices, all_drinks, left_on="drink", right_on = "DrinkName")
    db_client.insert_df(prices_drinks[['DrinkID', 'BarID','price']],"prices")

    #creating sales table
    sales_frames = [london, budapest, ny]
    all_sales = pd.concat(sales_frames)
    sales_drinks = pd.merge(all_sales, all_drinks, left_on='drink', right_on='DrinkName')
    db_client.insert_df(sales_drinks[['datetime','BarID','DrinkID']])

    #creating stock table
    bar_stock = pd.merge(bar_data, bars, left_on='bar', right_on='BarName')
    bar_glasses_stock = pd.merge(bar_stock, glasses[['GlassName','GlassID']], left_on="glass_type", right_on='GlassName')
    db_client.insert_df(bar_glasses_stock[['BarID','GlassID','stock']], "stock")

