from database_utils import DatabaseConnector
import pandas as pd
import sqlalchemy
import tabula
import requests

header = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

class DataExtractor():
    """
    This class extracts data from different data sources.
    e.g. CSV fies, API, S3 bucket.
    """
    def __init__(self):
        self.db_connector = DatabaseConnector('db_creds.yaml')
        self.db_engine = self.db_connector.db_engine
        self.db_creds = self.db_connector.db_creds

    def read_rds_table(self, table_name):
        table_data = pd.read_sql_table(table_name, self.db_engine).set_index('index')
        return table_data
    
    def retrieve_pdf_data(self, pdf_link):
        pdf_pages = tabula.read_pdf(pdf_link, pages='all')
        pdf_df = pd.concat(pdf_pages, ignore_index=True)        
        return pdf_df
    
    def list_number_of_stores(self, number_of_stores_endpoint, header):
        response = requests.get(number_of_stores_endpoint, headers=header)
        if response.status_code == 200:
            data = response.json()
            df = pd.json_normalize(data)  # Use json_normalize to flatten nested structures if present
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
        df = pd.DataFrame(all_store_data)
        return df
    


#number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
#extractor = DataExtractor()

#number_of_stores = extractor.list_number_of_stores(number_of_stores_endpoint,header)
#store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'

#print(all_stores_df.head())
#all_stores_df = extractor.retrieve_stores_data(store_endpoint, number_of_stores, header)