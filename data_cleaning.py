from data_extraction import *
from database_utils import *
import pandas as pd

class DataCleaning():
    """
    This class contains method to clean data from sources.
    """

    def __init__(self, table_name):
        self.connector = DatabaseConnector()
        self.extractor = DataExtractor()
        self.df = self.extractor.read_rds_table(table_name)
        
    def clean_user_data(self):
        # TO DO: double check this one
        # Drop NULL values and duplicates
        self.df = self.df.dropna().drop_duplicates()

        # Change columns to correct types
        self.df.first_name = self.df.first_name.astype('string')
        self.df.last_name = self.df.first_name.astype('string')
        self.df.date_of_birth = pd.to_datetime(self.df.date_of_birth, format='mixed', errors='coerce')
        self.df.company = self.df.company.astype('string')
        self.df.email_address = self.df.email_address.astype('string') # TO DO: would like to format email
        self.df.country = self.df.country.astype('string') # TO DO: check for spelling errors, get unique vals

        # Converting country_code to string, changing GGB to GB, and setting incorrect codes to NaN
        self.df.country_code = self.df.country_code.astype('string')
        self.df.country_code = self.df.country_code.replace('GGB', 'GB')
        self.df.loc[self.df.country_code.str.len() > 2, 'country_code'] = pd.NaT

        # Checking incorrect country codes have been removed
        # incorrect_country_codes = self.df[self.df['country_code'].str.len() > 2]
        # print(incorrect_country_codes)

        # TO DO: Converting phone number to correct regex expression
        # Getting rid of incorrect numbers, not integers

        # TO DO: other columns
        #self.df.info()

        # NULL values

        # Errors with dates

        # Incorrectly typed values

        # Rows filled with wrong information

example = DataCleaning('legacy_users')
print(example.clean_user_data())