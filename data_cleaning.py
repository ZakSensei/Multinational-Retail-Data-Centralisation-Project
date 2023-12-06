import pandas as pd

class DataCleaning():  
    
    def clean_user_data(self,df):
        df = df.drop_duplicates()
        df = df.dropna()
        df['country_code'] = df['country_code'].str.replace('GBB', 'GB')
        return df