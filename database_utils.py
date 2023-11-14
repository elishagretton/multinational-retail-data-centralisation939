from data_extraction import *
from data_cleaning import *
import yaml
from sqlalchemy import create_engine, inspect



class DatabaseConnector():
    """
    This class connects and uploads data to a database.
    """

    def __init__(self):
        self.db_creds = self.read_db_creds()
        self.db_engine = self.init_db_engine()
        
    def read_db_creds(self):
        file_path = 'db_creds.yaml'
        with open(file_path, 'r') as file:
            db_creds = yaml.safe_load(file)
        return db_creds
    
    def init_db_engine(self):
        database_url = (
        f"postgresql://{self.db_creds['RDS_USER']}:{self.db_creds['RDS_PASSWORD']}"
        f"@{self.db_creds['RDS_HOST']}:{self.db_creds['RDS_PORT']}/{self.db_creds['RDS_DATABASE']}"
        )
        db_engine = create_engine(database_url)
        return db_engine
    
    def list_db_tables(self):
        inspector = inspect(self.db_engine)
        db_table_list = inspector.get_table_names()
        return db_table_list

    def upload_to_db(self, clean_dataframe, table_name):
        db_to_sql = clean_dataframe.to_sql(table_name, self.db_engine, if_exists='replace', index=False)
        return db_to_sql




    
connector = DatabaseConnector()
