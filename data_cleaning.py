import pandas as pd

class DataCleaning():  
    
    def clean_user_data(self,df):
        df.drop("index", axis= "columns", inplace=True)
        df.drop(columns = "index", inplace = True)
        df = df.drop_duplicates(inplace = True)
        df = df.fillna('')
        
        
        #df['country_code'] = df['country_code'].str.replace('GBB', 'GB')
        return df