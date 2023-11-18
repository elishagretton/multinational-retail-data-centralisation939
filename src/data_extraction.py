from io import StringIO
from src.database_utils import DatabaseConnector
import boto3
import pandas as pd
import requests
import sqlalchemy
import tabula


header = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

class DataExtractor():
    """
    This class extracts data from different data sources.
    e.g. CSV fies, API, S3 bucket.
    """
    def __init__(self):
        """
        Initializes the DataExtractor instance.

        
        """
        self.db_connector = DatabaseConnector('../db_creds.yaml')
        self.db_engine = self.db_connector.db_engine
        self.db_creds = self.db_connector.db_creds

    def read_rds_table(self, table_name):
        table_data = pd.read_sql_table(table_name, self.db_engine).set_index('index')
        return table_data
    
    def retrieve_pdf_data(self, pdf_link):
        """
        Retrieves pdf file from S3 Bucket and returns a pandas DataFrame.

        Parameters:
        - pdf-link (string): Link to pdf in S3 Bucket.

        Returns: 
        - pdf_data (pandas DataFrame)
        """
        pdf_pages = tabula.read_pdf(pdf_link, pages='all')
        pdf_data = pd.concat(pdf_pages, ignore_index=True)        
        return pdf_data
    
    def list_number_of_stores(self, number_of_stores_endpoint, header):
        """
        Returns the number of stores in the data from API endpoint.
        
        Parameters:
        - number_of_stores_endpoint (str): endpoint URL for API
        - header (dict): credentials to connect to API
        
        Returns:
        - number_of_stores (int): number of stores in data
        """
        response = requests.get(number_of_stores_endpoint, headers=header)
        if response.status_code == 200:
            data = response.json()
            df = pd.json_normalize(data) 
            number_of_stores = df.number_stores[0]
            print(f'Number of stores: {number_of_stores}')
            return number_of_stores
        else:
            print(f'Request failed with status code: {response.status_code}')
            print(f'Response Text: { response.text}')

    def retrieve_stores_data(self, store_endpoint, number_of_stores, header):
        all_store_data = []
        for store_number in range(number_of_stores):
            response = requests.get(f'{store_endpoint}{store_number}', headers=header)
            if response.status_code == 200:
                store_data = response.json()
                all_store_data.append(store_data)
            else:
                print(f"Failed to fetch data for store {store_number}. Status code: {response.status_code}")
        store_data = pd.DataFrame(all_store_data)
        return store_data
    
    def extract_from_s3(self, s3_address):
        """
        
        """
        # Check valid s3 address
        if not s3_address.startswith('s3://'):
            raise ValueError("Invalid S3 address format. It should start with 's3://'")

        # Remove the 's3://' prefix and set access key id and key
        s3_address = s3_address[5:]

        # Split the remaining address into bucket name and file key
        parts = s3_address.split('/')
        if len(parts) < 2:
            raise ValueError("Invalid S3 address format. It should contain both bucket name and file key")

        bucket_name = parts[0]
        file_key = '/'.join(parts[1:])

        # Download the file from S3
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        data = response['Body']

        # Convert the CSV data to a Pandas DataFrame and remove duplicate index column.
        product_data = pd.read_csv(data, index_col='Unnamed: 0').reset_index(drop=True)
        return product_data
    