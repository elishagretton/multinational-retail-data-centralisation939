from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import pandas as pd
import re

class DataCleaning():
    """
    This class contains method to clean data from sources.
    """

    def __init__(self, uncleaned_df):
        self.db_connector = DatabaseConnector('db_creds.yaml')
        self.db_extractor = DataExtractor()
        self.uncleaned_df = uncleaned_df
        
    def clean_user_data(self):
        df = self.db_extractor.read_rds_table(self.uncleaned_df)

        # Drop null values in place
        df.dropna(inplace=True)

        # Drop duplicates in place
        df.drop_duplicates(inplace=True)

        # Change columns to correct types
        df.first_name = df.first_name.astype('string')
        df.last_name = df.first_name.astype('string')
        df.date_of_birth = pd.to_datetime(df.date_of_birth, format='mixed', errors='coerce')
        df.company = df.company.astype('string')
        df.email_address = df.email_address.astype('string') # TO DO: would like to format email
        df.country = df.country.astype('string') # TO DO: check for spelling errors, get unique vals
        df.address = df.address.astype('string') 
        df.join_date = pd.to_datetime(self.df.join_date, format='mixed', errors='coerce')
        df.user_uuid = df.user_uuid.astype('string')

        # Converting country_code to string, changing GGB to GB, and setting incorrect codes to NaN
        df.country_code = df.country_code.astype('string')
        df.country_code = df.country_code.replace('GGB', 'GB')
        df.loc[df.country_code.str.len() > 2, 'country_code'] = pd.NaT

        # Cleaning phone numbers 
        regex = '^(\(?\+?[0-9]*\)?)?[0-9_\- \(\)]*$'
            # Remove (0) in phone number
        df.loc[:,'phone_number'] = df.phone_number.str.replace('(0)', '', regex=False)
            # Remove all non-digits except when starts with + from phone numbers (to keep country codes)
        df.phone_number = df.phone_number.apply(lambda x: ''.join(char for char in x if char.isdigit() or (char == '+' and x.startswith('+'))))
        df.phone_number = df.phone_number.astype('string')

        return df
    
    def clean_card_data(self):
        # TO DO: IF UNCLEANED_DATA NOT A PDF THEN ERROR

        # Retrieve pdf_data
        pdf_data = self.db_extractor.retrieve_pdf_data(self.uncleaned_df)

        # Drop null values in place
        pdf_data.dropna(inplace=True)

        # Drop duplicates in place
        pdf_data.drop_duplicates(inplace=True)

        # Setting columns to correct data types
        pdf_data.card_number = pdf_data.card_number.astype('string')
        pdf_data.expiry_date = pd.to_datetime(pdf_data.expiry_date, format='%m/%y', errors='coerce')
        pdf_data.card_provider = pdf_data.card_provider.astype('string')
        pdf_data.date_payment_confirmed = pd.to_datetime(pdf_data.date_payment_confirmed, format='mixed', errors='coerce')

        return pdf_data

    def called_clean_store_data(self):
        # Set api_data to input
        api_data = self.uncleaned_df

        # Clean each column
        # (1) INDEX: Remove index column (duplicate)
        api_data = api_data.drop(columns='index')

        # (2) ADDRESS: Clean address column
        # Define a regular expression pattern for a basic address
        address_pattern = re.compile(r'\b\d+\s+\S+[^,]*,\s*\S+\b')
        # Apply the regex pattern using lambda function
        api_data.address= api_data.address.apply(lambda x: x if address_pattern.search(x) else None)
        # Set to string type
        api_data.address = api_data.address.astype('string')

        # (3) LONGITUDE: Set longitude to float
        api_data.longitude = pd.to_numeric(api_data.longitude, errors='coerce')
        
        # (4) LAT: Remove lat column (duplicate of latitude column)
        api_data = api_data.drop(columns='lat')

        # (5) LOCALITY: Clean locality column
        # Remove non-alphabetical and NULL values to keep relevant names
        api_data.locality= api_data.locality.apply(lambda x: x if pd.notnull(x) and x.isalpha() else pd.NaT)
        # Set to string type
        api_data.locality = api_data.locality.astype('string')

        # (6) STORE_CODE: Set store_code to string type
    # TO DO: is there a regex expression? looks like LA-jfkjkfej, but need to check.
        api_data.store_code = api_data.store_code.astype('string')

        # (6) STAFF_NUMBER: Clean staff_numbers
        # Getting rid of non-integers and numbers with spelling mistakes
        api_data.staff_numbers = api_data.staff_numbers.mask(~api_data.staff_numbers.str.match('^\d+$'))
        # Set to integer type
        api_data.staff_numbers = pd.to_numeric(api_data.staff_numbers, errors='coerce', downcast='integer')

        # (7) OPENING_DATE: Set to datetime and remove errors
        api_data.opening_date = pd.to_datetime(api_data.opening_date, format='mixed', errors='coerce')
        
        # (8) STORE_TYPE: Clean store_type
        # List desired stores
        desired_stores = ['Web Portal', 'Local', 'Super Store', 'Mall Kiosk', 'Outlet']
        # Filter out errors in store_type if not in desired_stores
        api_data.store_type = api_data.store_type.apply(lambda x: x if x in desired_stores else pd.NaT)
        # Set to string type
        api_data.store_type = api_data.store_type.astype('string')

        # (9) LATITUDE: Set latitude to float and remove errors
        api_data.latitude = pd.to_numeric(api_data.latitude, errors='coerce')

        # (10) COUNTRY_CODE: Clean country_code column
        # Set to string
        api_data.country_code = api_data.country_code.astype('string')
        # Remove strings longer 2, set to NaN
        api_data.loc[api_data.country_code.str.len() > 2, 'country_code'] = pd.NaT

        # (11) CONTINENT: Clean continent column
        # Set to string type
        api_data['continent'] = api_data['continent'].astype('string')
        # Replace spelling mistakes
        api_data.continent = api_data.continent.replace('eeEurope', 'Europe')
        api_data.continent = api_data.continent.replace('eeAmerica', 'America')
        # Remove strings containing numbers and 'NULL', set to NaN
        api_data.continent = api_data.continent.mask(api_data.continent.str.contains(r'\d|NULL'))

        return api_data
        
#instance = DatabaseConnector('sales_data_creds.yaml')
#instance.upload_to_db(pdf_data, 'dim_card_details')

# (1): Get stores pd dataframe
header = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
extractor = DataExtractor()
number_of_stores = extractor.list_number_of_stores(number_of_stores_endpoint,header)

store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'

all_stores_df = extractor.retrieve_stores_data(store_endpoint, number_of_stores, header)

# (2) Cleaning PDF data below        
example = DataCleaning(all_stores_df)
cleaned_df = example.called_clean_store_data()

instance = DatabaseConnector(file_path='sales_data_creds.yaml')
instance.upload_to_db(cleaned_df,'dim_store_details')


