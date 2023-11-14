from data_extraction import *
from database_utils import *
import pandas as pd

class DataCleaning():
    """
    This class contains method to clean data from sources.
    """

    def __init__(self, table_name):
        self.db_connector = DatabaseConnector()
        self.db_extractor = DataExtractor
        self.df = self.db_extractor.read_rds_table(table_name)
        
    def clean_user_data(self):
        # Drop null values in place
        self.df.dropna(inplace=True)

        # Drop duplicates in place
        self.df.drop_duplicates(inplace=True)

        # Change columns to correct types
        self.df.first_name = self.df.first_name.astype('string')
        self.df.last_name = self.df.first_name.astype('string')
        self.df.date_of_birth = pd.to_datetime(self.df.date_of_birth, format='mixed', errors='coerce')
        self.df.company = self.df.company.astype('string')
        self.df.email_address = self.df.email_address.astype('string') # TO DO: would like to format email
        self.df.country = self.df.country.astype('string') # TO DO: check for spelling errors, get unique vals
        self.df.address = self.df.address.astype('string') 
        self.df.join_date = pd.to_datetime(self.df.join_date, format='mixed', errors='coerce')
        self.df.user_uuid = self.df.user_uuid.astype('string')

        # Converting country_code to string, changing GGB to GB, and setting incorrect codes to NaN
        self.df.country_code = self.df.country_code.astype('string')
        self.df.country_code = self.df.country_code.replace('GGB', 'GB')
        self.df.loc[self.df.country_code.str.len() > 2, 'country_code'] = pd.NaT

        # Cleaning phone numbers 
        regex = '^(\(?\+?[0-9]*\)?)?[0-9_\- \(\)]*$'
            # Remove (0) in phone number
        self.df.loc[:,'phone_number'] = self.df.phone_number.str.replace('(0)', '', regex=False)
            # Remove all non-digits except when starts with + from phone numbers (to keep country codes)
        self.df.phone_number = self.df.phone_number.apply(lambda x: ''.join(char for char in x if char.isdigit() or (char == '+' and x.startswith('+'))))
        self.df.phone_number = self.df.phone_number.astype('string')

        return self.df


example = DataCleaning(db_connector=DatabaseConnector(), db_extractor=DataExtractor(db_connector=), table_name='legacy_users')
print(example.clean_user_data())
