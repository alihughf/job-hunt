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
        cur.executescript(script)
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


def return_glass(cocktail_client, drink_name):
    res = cocktail_client.get_info_for_cocktail_name(drink_name).json()
    return (res['drinks'][0]['strGlass'])

def main():
    cocktail_client = CocktailClient()
    db_client = SQLClient(database="bars.db")
    create_script = "data_tables.SQL"
    with open(create_script , 'r') as sql_file:
        sql_script = sql_file.read()

    db_client.build_tables(sql_script)

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
    db_client.insert_df(drinks_glass[['DrinkID','DrinkName','GlassID']], "drinks_dim")

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
    db_client.insert_df(sales_drinks[['datetime','BarID','DrinkID']], "sales")

    #creating stock table
    bar_stock = pd.merge(bar_data, bars, left_on='bar', right_on='BarName')
    bar_glasses_stock = pd.merge(bar_stock, glasses[['GlassName','GlassID']], left_on="glass_type", right_on='GlassName')
    db_client.insert_df(bar_glasses_stock[['BarID','GlassID','stock']], "stock")

if __name__ == "__main__":
    main()