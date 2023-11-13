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




    
instance = DatabaseConnector()

# Step 2: Print dictionary of credentials.
# print(instance.read_db_creds())

# Step 3: Create init_db_engine method, return sqlalchemy DB engine.
#instance.init_db_engine()

# Step 4: Create method list_db_tables.
#print(instance.list_db_tables())
