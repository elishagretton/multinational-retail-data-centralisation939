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

    def __init__(self):
        self.db_connector = DatabaseConnector('../db_creds.yaml')
        self.db_extractor = DataExtractor()
        
    def clean_user_data(self, user_data):
        """
        Cleans user_data Dataframe and returns clean dataframe.

        Parameters:
        - user_data (pd.DataFrame): user_data DataFrame to be cleaned.

        Returns:
        - user_data (pd.DataFrame): updated cleaned user_data DataFrame
        """
        # Check if user_data is a pandas DataFrame
        if not isinstance(user_data, pd.DataFrame):
            raise ValueError("Input 'user_data' must be a pandas DataFrame.")

        # Drop null values in place (none removed)
        user_data.dropna(inplace=True)

        # Drop duplicates in place (20 duplicates found)
        user_data.drop_duplicates(inplace=True)

        # Clean each column
        # (1) first_name: Set to string type
        user_data.first_name = user_data.first_name.astype('string')
       
        # (2) last_name: Set to string type
        user_data.last_name = user_data.last_name.astype('string')

        # (3) date_of_birth: Set to datetime
        user_data.date_of_birth = pd.to_datetime(user_data.date_of_birth, format='mixed', errors='coerce')
        
        # (4) company: Set to string type
        user_data.company = user_data.company.astype('string')
        
        # (5) email_address: Remove emails in incorrect format and set to string type
        email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        user_data.loc[~user_data['email_address'].str.match(email_pattern), 'email_address'] = np.nan
        user_data.email_address = user_data.email_address.astype('string')
        
        # (6) country: Remove incorrect countries (containing digits and 'NULL') and set to string type
        user_data.country = user_data.country .replace(['NULL', r'.*\d.*'], np.nan, regex=True)
        user_data.country = user_data.country.astype('string') 

        # (7) country_code: Replace 'GGB' to 'GB' and remove incorrect country_codes, set to string type
        user_data.country_code = user_data.country_code.replace('GGB', 'GB')
        user_data.loc[user_data.country_code.str.len() > 2, 'country_code'] = np.nan
        user_data.country_code = user_data.country_code.astype('string')

        # (8) phone_number: Remove '(0)', remove punctuation except when starts with '+', set to string type
        user_data.loc[:,'phone_number'] = user_data.phone_number.str.replace('(0)', '', regex=False)
        user_data.phone_number = user_data.phone_number.apply(lambda x: ''.join(char for char in x if char.isdigit() or (char == '+' and x.startswith('+'))))
        user_data.phone_number = user_data.phone_number.astype('string')

        # (9) join_date: Set to datetime
        user_data.join_date = pd.to_datetime(user_data.join_date, format='mixed', errors='coerce')

        # (10) user_uuid: Remove user_uuid in incorrect format, set to string type
        uuid_pattern = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        user_data.loc[~user_data['user_uuid'].str.match(uuid_pattern), 'user_uuid'] = np.nan
        user_data.user_uuid = user_data.user_uuid.astype('string')

        return user_data
    
    def clean_card_data(self, card_data):
        """
        Cleans card_data Dataframe and returns clean dataframe.

        Parameters:
        - card_data (pd.DataFrame): card_data DataFrame to be cleaned.

        Returns:
        - card_data (pd.DataFrame): updated cleaned card_data DataFrame
        """
        # Check if card_data is a pandas DataFrame
        if not isinstance(card_data, pd.DataFrame):
            raise ValueError("Input 'card_data' must be a pandas DataFrame.")

        # Drop null values in place (none removed)
        card_data.dropna(inplace=True)

        # Drop duplicates in place (10 detected)
        card_data.drop_duplicates(inplace=True)

        # Clean each column
        # (1) card_number
        # Set to string type
        card_data.card_number = card_data.card_number.astype('string')
        # Filter values that are not solely digits (invalid card numbers)
        incorrect_card_numbers = card_data[~card_data['card_number'].str.match(r'^\d+$', na=False)]
        # Set these to nan
        card_data.loc[incorrect_card_numbers.index, 'card_number'] = np.nan

        # (2) expiry_date: Set to datetime
        card_data.expiry_date = pd.to_datetime(card_data.expiry_date, format='%m/%y', errors='coerce')

        # (3) card_provider: Remove invalid card providers and set to string type
        card_providers = ['Diners Club / Carte Blanche', 'American Express', 'JCB 16 digit',
       'JCB 15 digit', 'Maestro', 'Mastercard', 'Discover',
       'VISA 19 digit', 'VISA 16 digit', 'VISA 13 digit']
        card_data.card_provider = card_data.card_provider.apply(lambda x: x if x in card_providers else np.nan)
        card_data.card_provider = card_data.card_provider.astype('string')

        # (4) date_payment_confirmed: Set to datetime
        card_data.date_payment_confirmed = pd.to_datetime(card_data.date_payment_confirmed, format='mixed', errors='coerce')
        
        return card_data

    def clean_store_data(self, store_data):
        # Check if store_data is a pandas DataFrame
        if not isinstance(store_data, pd.DataFrame):
            raise ValueError("Input 'store_data' must be a pandas DataFrame.")

        # Drop duplicates in place (2 detected)
        store_data.drop_duplicates(inplace=True)

        # Clean each column
        # (1)+(4) index, lat: Remove columns
        store_data = store_data.drop(columns=['index', 'lat'])

        # (2) address: Remove entries that do not contain '\n'
        store_data = store_data[store_data['address'].str.contains('\n', na=False)]

        # (3) longitude: Set longitude to float
        store_data.longitude = pd.to_numeric(store_data.longitude, errors='coerce')

        # (5) locality: Set locality to string, no incorrect/missing values
        store_data.locality = store_data.locality.astype('string')

        # (6) store_code: Set to correct format and set to string type
        store_data = store_data[store_data['store_code'].str.match(r'^.{2}-.{8}$', na=False)]
        store_data.store_code = store_data.store_code.astype('string')

        # (6) staff_numbers: Replace typos with correct value
        store_data['staff_numbers'] = store_data['staff_numbers'].replace({'J78': '78', '30e': '30', '80R': '80', 'A97': '97', '3n9': '39'})

        # (7) opening_data: Set to datetime
        store_data.opening_date = pd.to_datetime(store_data.opening_date, format='mixed', errors='coerce')
        
        # (8) store_type: Remove invalid store_types and set to string type
        store_types = ['Web Portal', 'Local', 'Super Store', 'Mall Kiosk', 'Outlet']
        store_data.store_type = store_data.store_type.apply(lambda x: x if x in store_types else np.nan)
        store_data.store_type = store_data.store_type.astype('string')

        # (9) latitude: Set latitude to float and remove errors
        store_data.latitude = pd.to_numeric(store_data.latitude, errors='coerce')

        # (10) country_code: Remove invalid country codes
        store_data.country_code = store_data.country_code.astype('string')
        store_data.loc[store_data.country_code.str.len() > 2, 'country_code'] = np.nan

        # (11) continent: Remove typos and set to string
        store_data['continent'] = store_data['continent'].astype('string')
        store_data.continent = store_data.continent.replace('eeEurope', 'Europe')
        store_data.continent = store_data.continent.replace('eeAmerica', 'America')

        return store_data
    
    def convert_product_weights(self, products_data):
        for index, entry in enumerate(products_data['weight']):
            if entry is not None and isinstance(entry, str):
                match = re.match(r'([\d.]+)\s*x?\s*([\d.]*)\s*([a-zA-Z]+)', entry)
                if match:
                    numeric_part1, numeric_part2, unit = match.groups()
                    numeric_part1 = float(numeric_part1)
                    if numeric_part2 == '' and unit.lower() == 'g':
                        products_data.at[index, 'weight'] = numeric_part1 / 1000
                    elif unit.lower() == 'ml':
                        products_data.at[index, 'weight'] = numeric_part1 / 1000
                    elif unit.lower() == 'kg':
                        products_data.at[index, 'weight'] = numeric_part1
                    elif numeric_part2 != '' and unit.lower() == 'g':
                        numeric_part2 = float(numeric_part2)
                        products_data.at[index, 'weight'] = numeric_part1 * numeric_part2 / 1000
                    else: 
                        products_data.at[index, 'weight'] = np.nan 
                else:
                    products_data.at[0, 'weight'] = np.nan  # Handle invalid or missing entries
        
        # Change object type to float type now data is cleaned.
        products_data.weight = pd.to_numeric(products_data.weight, errors='coerce')

        return products_data

    def clean_products_data(self, products_data):
        # Check if products_data is a pandas DataFrame
        if not isinstance(products_data, pd.DataFrame):
            raise ValueError("Input 'products_data' must be a pandas DataFrame.")

        # Drop NULL values (9 detected)
        products_data.dropna(inplace=True)

        # Drop duplicates (0 detected)
        products_data.drop_duplicates(inplace=True)

        # Clean columns
        # (1) product_name: Set to string type
        products_data.product_name = products_data.product_name.astype('string')
        
        # (2) product_price: Remove £ from product_price and set to float type
        products_data.product_price = products_data.product_price.str.replace('£', '')
        products_data.product_price = pd.to_numeric(products_data.product_price, errors='coerce')

        # (3) weights: Use convert_product_weights function
        products_data = self.convert_product_weights(products_data)

        # (4) category: Set incorrect categories to nan and set to string
        categories = ['toys-and-games', 'sports-and-leisure', 'pets', 'homeware', 'health-and-beauty',
                            'food-and-drink', 'diy']
        products_data.loc[~products_data['category'].isin(categories), 'category'] = np.nan
        products_data.category = products_data.category.astype('string')

        # (5) EAN: Set all invalid EANs to nan (if not 13 digit number) and set to string
        products_data.EAN = products_data.EAN.where(products_data.EAN.str.match(r'^\d{13}$'), np.nan)
        products_data.EAN = products_data.EAN.astype('string')

        # (6) Set date_added to date time
        products_data.date_added = pd.to_datetime(products_data.date_added, format='mixed', errors='coerce')

        # TO FIX (7) : Set all invalid UUIDs to nan and set type
        uuid_pattern = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        products_data.loc[~products_data['uuid'].str.match(uuid_pattern), 'uuid'] = np.nan
        products_data.uuid = products_data.uuid.astype('string')

        # (8): removed: Fix spelling mistake, remove incorrect removed statuses, and set to string
        products_data.country_code = products_data.country_code.replace('Still_avaliable', 'Still_available')
        removed_statuses = ['Still_available', 'Removed']
        products_data.loc[~products_data['removed'].isin(removed_statuses), 'removed'] = np.nan
        products_data.removed = products_data.removed.astype('string')

        return products_data
    
    def clean_orders_data(self, orders_data):
        # Check if orders_data is a pandas DataFrame
        if not isinstance(orders_data, pd.DataFrame):
            raise ValueError("Input 'orders_data' must be a pandas DataFrame.")

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