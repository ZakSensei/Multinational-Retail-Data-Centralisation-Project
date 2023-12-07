import yaml
import sqlalchemy
import pandas as pd
#from pandasgui import show
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

"""This is a class will connect us with and upload data to the database"""
class DatabaseConnector():
    def __init__(self, credential_file_path = "db_creds.yaml"):
        self.cred_dict = self.read_db_creds(credential_file_path)
        
    #this will read the credentials yaml file and return a dictionary of the credentials
    def read_db_creds(self,yaml_file_path):
        with open(yaml_file_path, "r") as file:
            credentials = yaml.safe_load(file)  
            return credentials
        
    
    #read the credentials from the return of read_db_creds and initialise and return an sqlalchemy database engine
    def init_db_engine(self):
        connection = f"postgresql://{self.cred_dict['DB_USER']}:{self.cred_dict['DB_PASSWORD']}" + \
        f"@{self.cred_dict['DB_HOST']}:{self.cred_dict['DB_PORT']}" + f"/{self.cred_dict['DB_DATABASE']}"
        
        engine = sqlalchemy.create_engine(connection)
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        return engine
    
    def list_db_tables(self):
        engine = self.init_db_engine()
        with engine.connect() as connection:
            inspector = sqlalchemy.inspect(engine)
            table_names = inspector.get_table_names()
            return table_names


    def upload_to_db(self, dataframe, table_name):
        dataframe.to_sql(table_name, engine, index=False, if_exists='replace')

if __name__ == "__main__":
    db_ext = DataExtractor()
    db_conn = DatabaseConnector()
    db_clean = DataCleaning()
    engine = db_conn.init_db_engine()

    print(db_conn.list_db_tables())
    print("\n")
    df = db_ext.read_rds_table("legacy_users",engine)
    df.drop("index", axis= "columns", inplace=True)
    df.to_csv(r"mrdc.csv", index=False)

    #print(df['country_code'].unique())
    #pd.set_option('display.max_columns', None)
    #print(df)
    #show(df)

