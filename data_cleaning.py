import pandas as pd
import numpy as np

class DataCleaning():
    """This is a class that will contain methods to clean data from each of the data sources."""  
    
    def clean_user_data(self,df):
        
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')
        df['email_address'] = np.where(~df['email_address'].str.contains("@", na=False), np.nan, df['email_address'])
        
        df.replace('', np.nan, inplace=True)
        df.replace('NULL', np.nan, inplace=True)
        df.drop_duplicates(inplace = True)
        df.dropna(inplace = True)
        df.reset_index(inplace=True)
        df.drop(columns = "index", inplace = True)
        
        return df
    
    def clean_card_data(self,df):
        pass
