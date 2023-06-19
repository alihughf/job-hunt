# Use this script to write the python needed to complete this task
import requests as re
import pandas as pd





class SQLClient():
    def __init__(
            self,
            server=None,
            database=None
    ):
        self.server = server
        self.database = database

    def build_tables(
            script
    ):
        pass

    def send_query(
            query
    ):
        pass

class APIClient():
    def __init__(
            self,
            url=None,
            request=None
    ):
        self.url = url
        self.request = request
    
    def send_request(
            url,
            request
    ):
        pass



