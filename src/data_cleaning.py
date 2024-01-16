from src.data_extraction import DataExtractor
from src.database_utils import DatabaseConnector
import pandas as pd
import numpy as np
#RESOLVED (13/12): Quick check to remove unused imports

#TODO: Quite a few of these methods do not require self as they function as helper methods for 
# cleaning specific columns, we could either make a helper function class or turn these into
#@staticmethods

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

    #NOTE: Again your commitment to performing checks like this method is brilliant &
    #will be a part interviewers will pick up on!
    @staticmethod
    def __check_input_is_pd(self, data):
        """
        Checks if the input is a pandas DataFrame.
        """
        if not isinstance(data, pd.DataFrame):
            raise ValueError("Input data must be a pandas DataFrame.")  
        
    #RESOLVED (13/12): Very nitpicky but these are protected methods at the moment, you just need to double
    # underscore them    
    # Private Methods involved in data cleaning of dataframes. 
    def __clean_addresses(self, df):
        """
        Cleans address column by removing entries that do not contain '\n'

        Parameters:
        - df (pd.DataFrame): dataframe containing address column to be cleaned.
        """
        df.loc[~df['address'].str.contains('\n', na=False), 'address'] = np.nan

    def __clean_card_numbers(self,df):
        #NOTE: Great to see regex being used here!
        """
        Clean card_number by setting to string, replacing '?', and removing strings containing letters.

        Parameters:
        - df (pd.DataFrame): dataframe containing card_number column to be cleaned.
        """
        df['card_number'] = df['card_number'].astype('string')
        df['card_number'] = df['card_number'].str.replace('?', '')
        df['card_number'] = df['card_number'].where(df['card_number'].str.contains(r'^\d+$'), np.nan)
    
    def __clean_categories(self, df, column_name, categories):
        """
        Cleans column_name by filtering out values not in categories.
        
        Parameters:
        - df (pd.DataFrame): dataframe containing column_name to be cleaned.
        - column_name (str): column containing categories to be cleaned.
        - categories (arr): categories that are valid in the column.
        """
        #NOTE: perfect usage of ~df, not many students do this!
        df.loc[~df[column_name].isin(categories), column_name] = np.nan
        df[column_name] = df[column_name].astype('string')

    def __clean_continents(self, df):
        """
        Cleans continent column by removing typos and then removing numerical values using _clean_string_data.
        
        Parameters
        - df (pd.DataFrame): dataframe containing continent column to be cleaned.
        """
        df['continent'] = df['continent'].astype('string')
        df.continent = df.continent.replace('eeEurope', 'Europe')
        df.continent = df.continent.replace('eeAmerica', 'America')
        df = self._clean_string_data(df, ['continent'])
    
    def __clean_country_code(self, df):
        """
        Cleans country_code column:
        - Replaces typos such as 'GGB'
        - Removes incorrect codes e.g. 'QVUW9JSKY3' by setting len
        - Sets to string type

        Parameters:
        - df (pd.DataFrame): dataframe containing country_code column to be cleaned.
        """
        #NOTE: Love converting invalid country_codes into np.nan
        df.country_code = df.country_code.replace('GGB', 'GB')
        df.loc[df.country_code.str.len() > 2, 'country_code'] = np.nan
        df.country_code = df.country_code.astype('string')
  
    def __clean_dates(self, df, columns):
        """
        Cleans date data by removing incorrectly formatted data and putting in the form Year-Month-Day.
        
        Parameters:
        - df (pd.DataFrame): dataframe containing columns to be cleaned.
        - columns (arr): array of column names to be cleaned.
        """
        #NOTE: YES, Thank you for having a reusable method for cleaning datatimes!!!!!!
        for column_name in columns:
            df[column_name] = pd.to_datetime(df[column_name], format='mixed', errors='coerce')
            df[column_name] = df[column_name].dt.strftime('%Y-%m-%d')
       
    def __clean_email_addresses(self, df):
        """
        Cleans email_address column by removing incorrectly formatted emails.

        Parameters:
        - df (pd.DataFrame): dataframe containing email_address column to be cleaned.
        """
        #NOTE: Lovely regex again!
        email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        df.loc[~df['email_address'].str.match(email_pattern), 'email_address'] = np.nan
        df.email_address = df.email_address.astype('string')
 
    def __clean_expiry_dates(self, df):
        """
        Clean expiry date columns in format %m/%y e.g. 01/12

        Parameters:
        - df (pd.DataFrame): dataframe containing expiry_date column to be cleaned.
        """
        df.loc[:,'expiry_date'] = pd.to_datetime(df['expiry_date'], errors = 'coerce', format='%m/%y')

    def __clean_number_data(self, df, columns):
        """
        Cleans number data by removing letters and setting to string.

        Parameters:
        - df (pd.DataFrame): dataframe containing columns to be cleaned.
        - columns (arr): array containing columns with number data to be removed of letters.
        """
        for column in columns:
            df[column] = pd.to_numeric(df[column], errors='coerce')
            df[column] = df[column].astype('string')
 
    def __clean_phone_numbers(self, df):
        """
        Cleans phone_number column by removing (0) and non-digit characters. 
        
        Parameters:
        - df (pd.DataFrame): dataframe containing phone_number column to be cleaned.
        """
        regex = '^(\(?\+?[0-9]*\)?)?[0-9_\- \(\)]*$'
        df.loc[:,'phone_number'] = df['phone_number'].str.replace('(0)', '', regex=False)
        df.loc[:,'phone_number'] = df['phone_number'].replace(r'\D+', '', regex=True)

    def __clean_product_codes(self,df):
        """
        Cleans product_code by removing incorrectly formatting codes.
        - 2 digits followed by hyphen followed by characters.
        
        Parameters:
        - df (pd.DataFrame): dataframe containing product_code column to be cleaned.
        """
        product_code_pattern = r'^.{2}-.*$'
        df.loc[~df['product_code'].str.match(product_code_pattern), 'product_code'] = np.nan
        df.product_code = df.product_code.astype('string')

    def __clean_product_price(self, df):
        """
        Cleans product_price by removing non-digits besides '£' and '.' and sets to string type.
        
        Parameters:
        - df (pd.DataFrame): dataframe containing product_price column to be cleaned.
        """
        df['product_price'] = df['product_price'].replace(to_replace=r'[^£.0-9]', value=np.nan, regex=True)
        df.product_price = df.product_price.astype('string')

    def __clean_product_weights(self, products_data):
        """
        Converts product weights in the provided products_data DataFrame to a consistent format.
        """
        #NOTE: This is a brilliant way to deal with the weight conversions, I would just be careful
        #of the reusability of line 188
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

    def __clean_staff_numbers(self, df):
        """
        Cleans staff_number by removing incorrect values and removing non-numerical entries.

        Parameters:
        - df (pd.DataFrame): dataframe containing staff_number column to be cleaned.
        """
        #RESOLVED: Here we should remove non-numeric chars, this line is not reusable at the moment
        df['staff_numbers'] = df['staff_numbers'].apply(lambda x: x if len(x) != 10 else np.nan)
        df['staff_numbers'] = df['staff_numbers'].str.replace(r'\D', '', regex=True)

    def __clean_store_codes(self, df):
        """
        Cleans store_code by removing incorrectly formatted entries.
        - 2-3 characters followed by hyphen followed by 8 characters

        Parameters:
        - df (pd.DataFrame): dataframe containing store_code column to be cleaned.
        """
        df.loc[~df['store_code'].str.match(r'^.{2,3}-.{8}$', na=False), 'store_code'] = np.nan
        df.store_code = df.store_code.astype('string')
                                   
    def __clean_string_data(self, df, columns):
        """
        Cleans string data by removing numerical and incorrect values and sets to correct type.

        Parameters:
        - df (pd.DataFrame): dataframe containing columns to be cleaned.
        - columns (arr): array of column names to be cleaned.
        """
        for column_name in columns:
            df.loc[df[column_name].str.contains('\d', na=False), column_name] = np.nan
            df.loc[:, column_name] = df[column_name].replace('NULL', np.nan)
            df.loc[:, column_name] = df[column_name].astype('string')

    def __clean_uuids(self, df, columns):
        """
        Removes incorrect entries with incorrect uuid format.

        Parameters:
        - df (pd.DataFrame): dataframe containing columns to be cleaned.
        - columns (arr): array of columns containing uuids to be cleaned.
        """
        for column_name in columns:    
            uuid_pattern = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
            df.loc[:,column_name] = df[column_name].astype('string')
            df.loc[~df[column_name].str.match(uuid_pattern), column_name] = np.nan
   
    # Public Methods to clean whole dataframes below
    #NOTE: Love the containrisation of all cleaning methods into overarching methods here!
    def clean_card_data(self, card_data):
        """
        Cleans the provided card_data DataFrame and returns the cleaned DataFrame.
        """
        self.__check_input_is_pd(card_data)
        card_data = card_data.dropna().drop_duplicates()

        self._clean_categories(card_data, 'card_provider', 
                               categories = ['Diners Club / Carte Blanche', 'American Express', 'JCB 16 digit',
        'JCB 15 digit', 'Maestro', 'Mastercard', 'Discover',
       'VISA 19 digit', 'VISA 16 digit', 'VISA 13 digit'])
        self._clean_card_numbers(card_data)
        self._clean_dates(card_data, ['date_payment_confirmed'])
        self._clean_expiry_dates(card_data)

        return card_data
        
    def clean_date_data(self, date_events_data):
        #NOTE: Most dont catch & clean this much data, truly an exceptional project!
        """
        Cleans the provided date_events_data DataFrame and returns the cleaned DataFrame.
        """
        self.__check_input_is_pd(date_events_data)
        date_events_data = date_events_data.dropna().drop_duplicates()

        self._clean_categories(date_events_data, 'time_period',
                               categories=['Evening', 'Morning', 'Midday', 'Late_Hours'])
        self._clean_number_data(date_events_data, ['year','month','day'])
        date_events_data.timestamp = pd.to_datetime(date_events_data.timestamp, format='%H:%M:%S', errors='coerce').dt.time
        self._clean_uuids(date_events_data, ['date_uuid'])

        return date_events_data
    
    def clean_orders_data(self, orders_data):
        """
        Cleans the provided orders_data DataFrame and returns the cleaned DataFrame.
        """
        self.__check_input_is_pd(orders_data)
        orders_data = orders_data.drop(columns=['level_0', 'first_name', 'last_name', '1'])
        orders_data = orders_data.drop_duplicates().dropna()

        self._clean_card_numbers(orders_data)
        self._clean_product_codes(orders_data)
        #NOTE: Perfect way to standerdise the data!
        orders_data.product_quantity = pd.to_numeric(orders_data.product_quantity, errors='coerce', downcast='integer')
        self._clean_store_codes(orders_data)
        self._clean_uuids(orders_data, ['date_uuid', 'user_uuid'])
        return orders_data

    def clean_products_data(self, products_data):
        """
        Cleans the provided products_data DataFrame and returns the cleaned DataFrame.
        """
        self.__check_input_is_pd(products_data)
        products_data = products_data.dropna().drop_duplicates()

        self.__clean_dates(products_data, ['date_added'])
        self.__clean_number_data(products_data, ['EAN'])
        self.__clean_product_codes(products_data)
        self.__clean_product_price(products_data)
        self.__clean_product_weights(products_data)
        products_data.product_name = products_data.product_name.astype('string')
        self.__clean_uuids(products_data, ['uuid'])
        self.__clean_categories(products_data, 
                               column_name = 'category', 
                               categories = ['toys-and-games', 'sports-and-leisure', 'pets', 'homeware', 'health-and-beauty',
                            'food-and-drink', 'diy'])
        
        products_data.removed = products_data.removed.replace('Still_avaliable', 'still_available')
        self.__clean_categories(products_data, 
                               column_name='removed', 
                               categories=['still_available', 'Removed'])

        return products_data
    
    def clean_store_data(self, store_data):
        """
        Cleans the provided store_data DataFrame and returns the cleaned DataFrame.
        """
        self.__check_input_is_pd(store_data)
        store_data = store_data.drop(columns=['index','lat'])
        store_data = store_data.drop_duplicates()

        self.__clean_addresses(store_data)
        self.__clean_categories(store_data, 'store_type', categories = ['Web Portal', 'Local', 'Super Store', 'Mall Kiosk', 'Outlet'])
        self.__clean_continents(store_data)        
        self.__clean_country_code(store_data)
        self.__clean_dates(store_data, ['opening_date'])
        self.__clean_number_data(store_data, ['longitude', 'latitude'])
        self.__clean_staff_numbers(store_data)
        self.__clean_store_codes(store_data)
        self.__clean_string_data(store_data, ['locality'])

        return store_data
    
    def clean_user_data(self, user_data):
        """
        Cleans the provided user_data DataFrame and returns the cleaned DataFrame.
        """
        self.__check_input_is_pd(user_data)
        user_data = user_data.dropna().drop_duplicates()

        self.__clean_addresses(user_data)
        self.__clean_country_code(user_data)
        self.__clean_dates(user_data, ['join_date', 'date_of_birth']) 
        self.__clean_email_addresses(user_data) 
        self.__clean_phone_numbers(user_data)
        self.__clean_string_data(user_data,['first_name', 'last_name', 'company','country'])
        self.__clean_uuids(user_data, ['user_uuid'])
        return user_data

if __name__ == "__main__":
    pass