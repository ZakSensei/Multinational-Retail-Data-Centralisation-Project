import pandas as pd
import numpy as np

class DataCleaning():
    """This is a class that will contain methods to clean data from each of the data sources.""" 
    
    def clean_user_data(self,df):
        df['first_name'] = df['first_name'].astype('string')
        df['last_name'] = df['last_name'].astype('string')
        df["date_of_birth"] = pd.to_datetime(df["date_of_birth"], infer_datetime_format=True, errors='coerce')
        df['company'] = df['company'].astype('category') 
        df['email_address'] = df['email_address'].astype('string')
        df['address'] = df['email_address'].astype('string')
        df['country'] = df['company'].astype('category')
        df['country_code'] = df['country_code'].astype('category')
        df['phone_number'] = df['phone_number'].astype('string')
        df["join_date"] = pd.to_datetime(df["join_date"], infer_datetime_format=True, errors='coerce')
        df['user_uuid'] = df['user_uuid'].astype('string')
    
        df['email_address'] = np.where(~df['email_address'].str.contains("@", na=False), np.nan, df['email_address'])
        df['phone_number'].replace('.',np.nan, inplace=True)
        df.replace('', np.nan, inplace=True)
        df.replace('NULL', np.nan, inplace=True)

        df.drop_duplicates(inplace = True)
        df.dropna(inplace = True)
        df.reset_index(inplace=True)
        df.drop(columns = "index", inplace = True)
        return df
    
    def clean_card_data(self,df):
        pass
