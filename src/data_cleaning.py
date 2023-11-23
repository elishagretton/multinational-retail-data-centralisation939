from src.data_extraction import DataExtractor
from src.database_utils import DatabaseConnector
import pandas as pd
import numpy as np
import ast

class DataCleaning():
    """
    This class contains methods to clean data from various sources.
    """

    def __init__(self):
        """
        Initializes the DataCleaning instance.
        """
        self.db_connector = DatabaseConnector('../../db_creds.yaml')
        self.db_extractor = DataExtractor()

    def _check_input_is_pd(self,data):
        """
        Checks if the input is a pandas DataFrame.
        """
        if not isinstance(data, pd.DataFrame):
            raise ValueError("Input data must be a pandas DataFrame.")  
    
    def _clean_string_data(self, df, columns):
        """
        Cleans string data by removing numerical and incorrect values and sets to correct type.
        """
        for column_name in columns:
            df.loc[df[column_name].str.contains('\d', na=False), column_name] = np.nan
            df.loc[:, column_name] = df[column_name].replace('NULL', np.nan)
            df.loc[:, column_name] = df[column_name].astype('string')
    
    def _clean_dates(self, df, columns):
        """
        Cleans date data by removing incorrectly formatted data.
        """
        for column_name in columns:
            df[column_name] = pd.to_datetime(df[column_name], format='mixed', errors='coerce')
            df[column_name] = df[column_name].dt.strftime('%Y-%m-%d')

    def _clean_phone_numbers(self, user_data):
        """
        Cleans phone_number column by removing (0) and non-digit characters. 
        """
        regex = '^(\(?\+?[0-9]*\)?)?[0-9_\- \(\)]*$'
        user_data.loc[:,'phone_number'] = user_data['phone_number'].str.replace('(0)', '', regex=False)
        user_data.loc[:,'phone_number'] = user_data['phone_number'].replace(r'\D+', '', regex=True)

    def _clean_email_addresses(self, user_data):
        """
        Cleans email_address column by removing incorrectly formatted emails.
        """
        email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        user_data.loc[~user_data['email_address'].str.match(email_pattern), 'email_address'] = np.nan
        user_data.email_address = user_data.email_address.astype('string')
    
    def _clean_country_code(self, df):
        """
        Cleans country_code column:
        - Replaces typos such as 'GGB'
        - Removes incorrect codes e.g. 'QVUW9JSKY3' by setting len
        - Sets to string type
        """
        df.country_code = df.country_code.replace('GGB', 'GB')
        df.loc[df.country_code.str.len() > 2, 'country_code'] = np.nan
        df.country_code = df.country_code.astype('string')

    def _clean_uuids(self, df, columns):
        """
        Removes incorrect entries with incorrect uuid format.
        """
        for column_name in columns:    
            uuid_pattern = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
            df.loc[:,column_name] = df[column_name].astype('string')
            df.loc[~df[column_name].str.match(uuid_pattern), column_name] = np.nan

    def _clean_addresses(self, df, columns):
        """
        Cleans address column by removing entries that do not contain '\n'
        """
        for column_name in columns:
            df.loc[~df[column_name].str.contains('\n', na=False), column_name] = np.nan
    
    def clean_user_data(self, user_data):
        """
        Cleans the provided user_data DataFrame and returns the cleaned DataFrame.
        """
        self._check_input_is_pd(user_data)
        user_data = user_data.dropna().drop_duplicates()
        user_data = self._clean_string_data(user_data,['first_name', 'last_name', 'company','country'])
        user_data = self._clean_dates(user_data, ['join_date', 'date_of_birth']) 
        user_data = self._clean_uuids(user_data, ['user_uuid'])
        user_data = self._clean_addresses(user_data, ['address'])
        user_data = self._clean_country_code(user_data)
        user_data = self._clean_phone_numbers(user_data) 
        user_data = self._clean_email_addresses(user_data) 

        return user_data
    
    def _clean_number_data(self, df, columns):
        """
        Cleans number data by removing letters and setting to string.
        """
        for column in columns:
            df[column] = pd.to_numeric(df[column], errors='coerce')
            df[column] = df[column].astype('string')

    def _clean_card_numbers(self,df):
        """
        Clean card_number by setting to string, replacing '?', and removing strings containing letters.
        """
        df['card_number']=df['card_number'].astype('string')
        df['card_number'] = df['card_number'].str.replace('?', '')
        df['card_number'] = df['card_number'].where(df['card_number'].str.contains(r'^\d+$'), np.nan)

    def _clean_store_codes(self, df):
        """
        Cleans store_code by removing incorrectly formatted entries.
        """
        df.loc[~df['store_code'].str.match(r'^.{2,3}-.{8}$', na=False), 'store_code'] = np.nan
        df.store_code = df.store_code.astype('string')
                                   
    def _clean_staff_numbers(self, df):
        """
        Cleans staff_number by removing typos and removing non-numerical entries.
        """
        df['staff_numbers'] = df['staff_numbers'].replace({'J78': '78', '30e': '30', '80R': '80', 'A97': '97', '3n9': '39'})
        df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors='coerce').fillna(0).astype(int)
        return df
    
    def _clean_store_type(self, df):
        store_types = ['Web Portal', 'Local', 'Super Store', 'Mall Kiosk', 'Outlet']
        df.store_type = df.store_type.apply(lambda x: x if x in store_types else np.nan)
        df.store_type = df.store_type.astype('string')
        return df
    
    def _clean_continents(self, df):
        df['continent'] = df['continent'].astype('string')
        df.continent = df.continent.replace('eeEurope', 'Europe')
        df.continent = df.continent.replace('eeAmerica', 'America')
        df = self._clean_string_data(df, ['continent'])
    
    def _clean_categories(self, df, column_name, categories):
        df.loc[~df[column_name].isin(categories), column_name] = np.nan
        df.category = df[column_name].astype('string')

    def clean_store_data(self, store_data):
        """
        Cleans the provided store_data DataFrame and returns the cleaned DataFrame.
        """
        self._check_input_is_pd(store_data)
        store_data = store_data.drop_duplicates()
        store_data = store_data.drop(columns=['index','lat'])
        self._clean_addresses(store_data, ['address'])
        self._clean_string_data(store_data, ['locality'])
        self._clean_dates(store_data, ['opening_date'])
        self._clean_number_data(store_data, ['longitude', 'latitude'])
        self._clean_country_code(store_data)
        self._clean_store_codes(store_data)
        self._clean_staff_numbers(store_data)
        self._clean_continents(store_data)

        store_types = ['Web Portal', 'Local', 'Super Store', 'Mall Kiosk', 'Outlet']
        self._clean_categories(store_data, 'store_type', store_types)

        return store_data
    
    def _clean_expiry_dates(self, card_data):
        """
        Clean expiry date columns in format %m/%y e.g. 01/12
        """
        card_data.loc[:,'expiry_date'] = pd.to_datetime(card_data['expiry_date'], errors = 'coerce', format='%m/%y')


    def clean_card_data(self, card_data):
        """
        Cleans the provided card_data DataFrame and returns the cleaned DataFrame.
        """
        self._check_input_is_pd(card_data)
        card_data = card_data.dropna().drop_duplicates()
        self._clean_card_numbers(card_data)
        self._clean_expiry_dates(card_data)
        self._clean_dates(card_data, ['date_payment_confirmed'])
        card_providers = ['Diners Club / Carte Blanche', 'American Express', 'JCB 16 digit',
       'JCB 15 digit', 'Maestro', 'Mastercard', 'Discover',
       'VISA 19 digit', 'VISA 16 digit', 'VISA 13 digit']
        self._clean_categories(card_data, 'card_provider', card_providers)

        return card_data


    def _clean_product_weights(self, products_data):
        """
        Converts product weights in the provided products_data DataFrame to a consistent format.
        """
        replacements = {
            'kg': '',
            'g': '/1000',
            'ml': '/1000',
            'x': '*',
            'oz': '/35.274',
            '77/1000 .': '77/1000'
        }
        products_data.loc[:, 'weight'] = products_data['weight'].replace(replacements, regex=True)
        def evaluate_expression(expression):
            try:
                result = eval(expression)
                return float(result) if '/' in expression else result
            except Exception:
                return np.nan
            
        products_data['weight'] = products_data['weight'].apply(evaluate_expression)

    def _clean_product_price(self, products_data):
        """
        Cleans product_price by removing non-digits besides '£' and '.' and sets to string type.
        """
        products_data['product_price'] = products_data['product_price'].replace(to_replace=r'[^£.0-9]', value=np.nan, regex=True)
        products_data.product_price = products_data.product_price.astype('string')

    def _clean_product_codes(self,df):
        product_code_pattern = r'^.{2}-.*$'
        df.loc[~df['product_code'].str.match(product_code_pattern), 'product_code'] = np.nan
        df.product_code = df.product_code.astype('string')

    def clean_products_data(self, products_data):
        """
        Cleans the provided products_data DataFrame and returns the cleaned DataFrame.
        """
        self._check_input_is_pd(products_data)
        products_data = products_data.dropna().drop_duplicates()

        self._clean_string_data(products_data, ['product_name'])
        self._clean_product_weights(products_data)
        self._clean_product_price(products_data)
        self._clean_uuids(products_data, ['uuid'])
        self._clean_number_data(products_data, ['EAN'])
        self._clean_dates(products_data, ['date_added'])
        self._clean_product_codes(products_data)

        self._clean_categories(products_data, 
                               column_name = 'category', 
                               categories = ['toys-and-games', 'sports-and-leisure', 'pets', 'homeware', 'health-and-beauty',
                            'food-and-drink', 'diy'])
        
        products_data.removed = products_data.removed.replace('Still_avaliable', 'still_available')
        self._clean_categories(products_data, 
                               column_name='removed', 
                               categories=['still_available', 'Removed'])

        return products_data

    def clean_orders_data(self, orders_data):
        """
        Cleans the provided orders_data DataFrame and returns the cleaned DataFrame.

        Parameters:
        - orders_data (pd.DataFrame): DataFrame containing order data to be cleaned.

        Returns:
        - orders_data (pd.DataFrame): Cleaned orders_data DataFrame.
        """
        self._check_input_is_pd(orders_data)
        orders_data = orders_data.drop(columns=['level_0', 'first_name', 'last_name', '1'])
        orders_data = orders_data.drop_duplicates().dropna()
        self._clean_uuids(orders_data, ['date_uuid', 'user_uuid'])
        self._clean_card_numbers(orders_data)
        self._clean_store_codes(orders_data)
        self._clean_product_codes(orders_data)
        orders_data.product_quantity = pd.to_numeric(orders_data.product_quantity, errors='coerce', downcast='integer')

        return orders_data
    
    def clean_date_data(self, date_events_data):
        """
        Cleans the provided date_events_data DataFrame and returns the cleaned DataFrame.
        """
        self._check_input_is_pd(date_events_data)
        date_events_data = date_events_data.dropna().drop_duplicates()
        date_events_data.timestamp = pd.to_datetime(date_events_data.timestamp, format='%H:%M:%S', errors='coerce').dt.time
        self._clean_uuids(date_events_data, ['date_uuid'])
        self._clean_number_data(date_events_data, ['year','month','day'])
        self._clean_categories(date_events_data, 'time_period',
                               categories=['Evening', 'Morning', 'Midday', 'Late_Hours'])

        return date_events_data
    
if __name__ == "__main__":
    pass