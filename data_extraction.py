from database_utils import *
import pandas as pd
import sqlalchemy


class DataExtractor():
    """
    This class extracts data from different data sources.
    e.g. CSV fies, API, S3 bucket.
    """
    def __init__(self, db_connector):
        self.db_connector = db_connector
        self.db_engine = self.db_connector.db_engine
        self.db_creds = self.db_connector.db_creds

    def read_rds_table(self, table_name):
        table_data = pd.read_sql_table(table_name, self.db_engine).set_index('index')
        return table_data

extractor = DataExtractor(DatabaseConnector())

#print(extractor.read_rds_table('legacy_users')['address'])
