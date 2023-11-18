from sqlalchemy import create_engine, inspect
import yaml

class DatabaseConnector():
    """
    This class connects to a PostgreSQL database and provides methods to interact with it.
    
    Parameters:
    - file_path (str): Path to the YAML file containing database credentials.
    """

    def __init__(self, file_path):
        """
        Initializes the DatabaseConnector instance.

        Parameters:
        - file_path (str): Path to the YAML file containing database credentials.
        """  
        self.file_path = file_path
        self.db_creds = self.read_db_creds()
        self.db_engine = self.init_db_engine()
        
    def read_db_creds(self):
        """
        Reads and returns the database credentials from the specified YAML file.

        Returns:
        - dict: Database credentials.
        """
        with open(self.file_path, 'r') as file:
            db_creds = yaml.safe_load(file)
        return db_creds
    
    def init_db_engine(self):
        """
        Initializes and returns a SQLAlchemy database engine using the provided credentials.

        Returns:
        - sqlalchemy.engine.base.Engine: Database engine.
        """
        database_url = (
        f"postgresql://{self.db_creds['RDS_USER']}:{self.db_creds['RDS_PASSWORD']}"
        f"@{self.db_creds['RDS_HOST']}:{self.db_creds['RDS_PORT']}/{self.db_creds['RDS_DATABASE']}"
        )
        db_engine = create_engine(database_url)
        return db_engine
    
    def list_db_tables(self):
        """
        Returns a list of table names present in the connected database.

        Returns:
        - list: List of table names.
        """
        inspector = inspect(self.db_engine)
        db_table_list = inspector.get_table_names()
        return db_table_list

    def upload_to_db(self, clean_dataframe, table_name):
        """
        Uploads a clean DataFrame to the specified table in the connected database.

        Parameters:
        - clean_dataframe (pd.DataFrame): Cleaned DataFrame to be uploaded.
        - table_name (str): Name of the database table to which data will be uploaded.

        Returns:
        - str: Result of the DataFrame upload process.
        """
        db_to_sql = clean_dataframe.to_sql(table_name, self.db_engine, if_exists='replace', index=False)
        return db_to_sql

instance = DatabaseConnector('../db_creds.yaml')
instance.read_db_creds()