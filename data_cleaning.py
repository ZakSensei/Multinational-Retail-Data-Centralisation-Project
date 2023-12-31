import pandas as pd
import numpy as np

class DataCleaning():
    """This is a class that will contain methods to clean data from each of the data sources."""

    def remove_null_values(self, df):
        """
        Replace empty strings and 'NULL' values with NaN and drop rows with NaN values.

        Parameters:
        - df (DataFrame): The input DataFrame to perform the cleaning on.
        """
        df.replace('', np.nan, inplace=True)
        df.replace('NULL', np.nan, inplace=True)
        df.dropna(inplace=True)

    def reset_index(self, df):
        """
        Reset the index of the DataFrame and drop the 'index' column.

        Parameters:
        - df (DataFrame): The input DataFrame to reset the index on.
        """
        df.reset_index(inplace=True)
        df.drop(columns="index", inplace=True)

    def clean_user_data(self, users_df):
        """
        Clean the user data in the legacy_users DataFrame.

        Parameters:
        - users_df (DataFrame): The DataFrame containing user data to be cleaned.

        Returns:
        - DataFrame: The cleaned user DataFrame.
        """
        # Convert data types
        users_df['first_name'] = users_df['first_name'].astype('string')
        users_df['last_name'] = users_df['last_name'].astype('string')
        users_df["date_of_birth"] = pd.to_datetime(users_df["date_of_birth"], infer_datetime_format=True, errors='coerce')
        users_df['company'] = users_df['company'].astype('category')
        users_df['email_address'] = users_df['email_address'].astype('string')
        users_df['address'] = users_df['address'].astype('string')
        users_df['country'] = users_df['country'].astype('category')
        users_df['country_code'] = users_df['country_code'].astype('string')
        users_df['phone_number'] = users_df['phone_number'].astype('string')
        users_df["join_date"] = pd.to_datetime(users_df["join_date"], infer_datetime_format=True, errors='coerce')
        users_df['user_uuid'] = users_df['user_uuid'].astype('string')

        # Clean email_address and phone_number columns
        users_df['email_address'] = np.where(~users_df['email_address'].str.contains("@", na=False), np.nan,
                                              users_df['email_address'])
        users_df['phone_number'].replace('.', np.nan, inplace=True)

        # Drop duplicate rows and null values
        users_df.drop_duplicates(inplace=True)
        self.remove_null_values(users_df)
        self.reset_index(users_df)

        return users_df

    
    def clean_card_data(self,card_details_df):

        # Remove the last 14 values in 'card_provider which are mistakes'
        unique_card_providers = card_details_df['card_provider'].unique().tolist()
        card_providers_to_remove = unique_card_providers[-14:]
        card_details_df = card_details_df[~card_details_df['card_provider'].isin(card_providers_to_remove)].copy()


        # Remove duplicate rows
        card_details_df.drop_duplicates(inplace = True)
        
        # Filter rows based on specific card providers      
        all_card_providers = ["VISA 16 digit", "JCB 16 digit", "VISA 13 digit", "VISA 19 digit", "JCB 15 digit", "Diners Club / Carte Blanche", "American Express", "Maestro", "Discover", "Mastercard"]
        card_details_df = card_details_df[card_details_df.card_provider.isin(all_card_providers)].copy()


        # Replace 'NULL' with NaN and drop NaN values
        self.remove_null_values(card_details_df)

        # Filter out wrongly formatted date values
        date_values_to_remove = ['December 2021 17', 'December 2000 01', '2008 May 11', 'May 1998 09', '2005 July 01', 'September 2016 04', 'October 2000 04', '2017/05/15']
        card_details_df = card_details_df[~card_details_df['date_payment_confirmed'].isin(date_values_to_remove)].copy()

        # Convert date columns to datetime
        card_details_df["expiry_date"] = pd.to_datetime(card_details_df["expiry_date"], format='%m/%y')
        card_details_df["date_payment_confirmed"] = pd.to_datetime(card_details_df["date_payment_confirmed"], format='%Y-%m-%d')

        # Reset index
        card_details_df = card_details_df.reset_index(drop=True)
        self.reset_index(card_details_df)

        return card_details_df

    def clean_store_data(self, store_df):
        """
        Clean and preprocess store data in the given DataFrame.

        Parameters:
        - store_df (pd.DataFrame): DataFrame containing store data.

        Returns:
        - pd.DataFrame: Cleaned store data DataFrame.
        """

        # Drop unnecessary columns
        store_df.drop('lat', axis=1, inplace=True)

        # Remove null values
        self.remove_null_values(store_df)

        # Filter by country code
        valid_country_codes = ["GB", "DE", "US"]
        store_df = store_df[store_df['country_code'].isin(valid_country_codes)].copy()

        # Filter by store types
        valid_store_types = ["Super Store", "Web Portal", "Local"]
        store_df = store_df[store_df['store_type'].isin(valid_store_types)].copy()

        # Filter by continent
        #FIXME replace eeEurope and eeAmerica
        valid_continent_types = ["Europe", "America", "eeEurope", "eeAmerica"]
        store_df = store_df[store_df['continent'].isin(valid_continent_types)].copy()

        # Remove duplicate rows
        store_df.drop_duplicates(inplace=True)

        # Filter out wrongly formatted date values
        date_values_to_remove = ['July 2015 14', 'May 2003 27', '2016 November 25', 'October 2006 04', 'February 2009 28']
        store_df = store_df[~store_df['opening_date'].isin(date_values_to_remove)].copy()

        # Convert opening date to datetime
        store_df["opening_date"] = pd.to_datetime(store_df["opening_date"], infer_datetime_format=True, errors='coerce')

        # Replace 'N/A' and empty values with NaN and drop NaN values
        self.remove_null_values(store_df)

        # Convert latitude and longitude to numeric, rounding to two decimal places
        store_df['latitude'] = pd.to_numeric(store_df['latitude'], errors='coerce').round(2)
        store_df['longitude'] = pd.to_numeric(store_df['longitude'], errors='coerce').round(2)

        

        # Reset index
        self.reset_index(store_df)

        return store_df
