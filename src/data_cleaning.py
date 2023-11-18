from src.data_extraction import DataExtractor
from src.database_utils import DatabaseConnector
import pandas as pd
import re
import numpy as np
import uuid


# TO DO: 
# (1) sort inputs
# (2) drop duplicates and null - check and ask
# (3) to dos in each function
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
    
    def convert_product_weights(self, products_df):
        for index, entry in enumerate(products_df['weight']):
            if entry is not None and isinstance(entry, str):
                match = re.match(r'([\d.]+)\s*x?\s*([\d.]*)\s*([a-zA-Z]+)', entry)
                if match:
                    numeric_part1, numeric_part2, unit = match.groups()
                    numeric_part1 = float(numeric_part1)
                    if numeric_part2 == '' and unit.lower() == 'g':
                        products_df.at[index, 'weight'] = numeric_part1 / 1000
                    elif unit.lower() == 'ml':
                        products_df.at[index, 'weight'] = numeric_part1 / 1000
                    elif unit.lower() == 'kg':
                        products_df.at[index, 'weight'] = numeric_part1
                    elif numeric_part2 != '' and unit.lower() == 'g':
                        numeric_part2 = float(numeric_part2)
                        products_df.at[index, 'weight'] = numeric_part1 * numeric_part2 / 1000
                    else: 
                        products_df.at[index, 'weight'] = np.nan 
                else:
                    products_df.at[0, 'weight'] = np.nan  # Handle invalid or missing entries
        
        # Change object type to float type now data is cleaned.
        products_df.weight = pd.to_numeric(products_df.weight, errors='coerce')

        return products_df

    def clean_products_data(self, products_df):

        # (1) Set product_name to string type
        products_df.product_name = products_df.product_name.astype('string')
        
        # (2) Remove £ from product_price and set to float type
        products_df.product_price = products_df.product_price.str.replace('£', '')
        products_df.product_price = pd.to_numeric(products_df.product_price, errors='coerce')

        # (3) Use convert_product_weights function
        products_df = self.convert_product_weights(products_df)

        # (4) Set incorrect categories to nan and set to string
        allowed_categories = ['toys-and-games', 'sports-and-leisure', 'pets', 'homeware', 'health-and-beauty',
                            'food-and-drink', 'diy']
        products_df.loc[~products_df['category'].isin(allowed_categories), 'category'] = np.nan
        products_df.category = products_df.category.astype('string')

        # (5) Set all invalid EANs to nan (if not 13 digit number) and set to string
        products_df.EAN = products_df.EAN.where(products_df.EAN.str.match(r'^\d{13}$'), np.nan)
        products_df.EAN = products_df.EAN.astype('string')

        # (6) Set date_added to date time
        products_df.date_added = pd.to_datetime(products_df.date_added, format='mixed', errors='coerce')

        # TO FIX (7) : Set all invalid UUIDs to nan and set type
        # products_df.uuid= products_df.uuid.apply(lambda x: x if pd.notna(x) and uuid.UUID(x) else np.nan)

        # (8): Set incorrect removed statuses to nan and set to string
        # TO DO : SPELLING MISTAKE, CHECK
        removed_statuses = ['Still_avaliable', 'Removed']
        products_df.loc[~products_df['removed'].isin(removed_statuses), 'removed'] = np.nan
        products_df.removed = products_df.removed.astype('string')

        return products_df
    
    def clean_orders_data(self, orders_data):
        # Remove unnecessary columns
        orders_data = orders_data.drop(columns=['level_0', 'first_name', 'last_name', '1'])

        # (1) date_uuid
        pattern = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        orders_data.loc[~orders_data['date_uuid'].str.match(pattern), 'date_uuid'] = np.nan
        orders_data.date_uuid = orders_data.date_uuid.astype('string')

        # (2) user_uuid
        orders_data.loc[~orders_data['user_uuid'].str.match(pattern), 'user_uuid'] = np.nan
        orders_data.user_uuid = orders_data.user_uuid.astype('string')

        # (3) card_number
        orders_data.card_number = pd.to_numeric(orders_data.card_number, errors='coerce', downcast='integer')

        # (4) store_code - already clean
        pattern = r'^.{2,3}-.{8}$'
        orders_data.loc[~orders_data['store_code'].str.match(pattern), 'store_code'] = np.nan
        orders_data.store_code = orders_data.store_code.astype('string')

        # (5) product_code
        pattern = r'^.{2}-.*$'
        orders_data.loc[~orders_data['product_code'].str.match(pattern), 'product_code'] = np.nan
        orders_data.product_code = orders_data.product_code.astype('string')

        # (6) product_quantity - clean already
        orders_data.product_quantity = pd.to_numeric(orders_data.product_quantity, errors='coerce', downcast='integer')

        return orders_data
    def clean_date_data(self, date_details_data):
        # Drop NULL and duplicates
        date_details_data.dropna(inplace=True)
        date_details_data.drop_duplicates(inplace=True)

        # (1) timestamp
        date_details_data.timestamp = pd.to_datetime(date_details_data.timestamp, format='%H:%M:%S', errors='coerce').dt.time

        # (2) month: Filter out incorrect data and set to float type
        pattern = r'^\d{1,2}$'
        date_details_data.loc[~date_details_data['month'].str.match(pattern), 'month'] = np.nan 
        date_details_data.month = pd.to_numeric(date_details_data.month, errors='coerce')

        # (3) year
        pattern = r'^\d{4}$'
        date_details_data.loc[~date_details_data['year'].str.match(pattern), 'year'] = np.nan 
        date_details_data.year = pd.to_numeric(date_details_data.year, errors='coerce')

        # (4) day
        pattern = r'^\d{1,2}$'
        date_details_data.loc[~date_details_data['day'].str.match(pattern), 'day'] = np.nan 
        date_details_data.month = pd.to_numeric(date_details_data.day, errors='coerce')


        # (5) time_period
        time_periods = ['Evening', 'Morning', 'Midday', 'Late_Hours']
        date_details_data.loc[~date_details_data['time_period'].isin(time_periods), 'time_period'] = np.nan
        date_details_data.time_period = date_details_data.time_period.astype('string')

        # (6) date_uuid
        pattern = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        date_details_data.loc[~date_details_data['date_uuid'].str.match(pattern), 'date_uuid'] = np.nan
        date_details_data.date_uuid = date_details_data.date_uuid.astype('string')

        return date_details_data