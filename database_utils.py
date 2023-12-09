from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from pandasgui import show
import sqlalchemy
import yaml

class DatabaseConnector():
    """This is a class that will contain methods to connect us with and upload data to the database"""

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
        localdb_conn = DatabaseConnector("localdb_creds.yaml")
        engine = localdb_conn.init_db_engine()
        dataframe.to_sql(table_name, engine, index=False, if_exists='replace')

if __name__ == "__main__":
    db_ext = DataExtractor()
    db_clean = DataCleaning()
    db_conn = DatabaseConnector()
    engine = db_conn.init_db_engine()                 #Creates engine

    card_details_df = db_ext.retrieve_pdf_data()      #Converts card details into dataframe (df)
    df = db_ext.read_rds_table("legacy_users",engine) #converts {table_name} into dataframe
    df.to_csv("mrdc.csv", index=False)                #converts the df to csv file 'mrdc.csv'

    df = db_clean.clean_user_data(df)                 #Cleans the dataframe
    db_conn.upload_to_db(df,'dim_users')              #uploads to local database
    
    print(f"{db_conn.list_db_tables()} \n")           #prints all table names in rds database
    show(df)                                          #Gui of the dataframe


    #Testing Code
    print(df['country_code'].unique())
    df['country_code'].replace('GBB', 'GB', inplace=True)
    print(df['country_code'].unique())
    print(df['country_code'].dtypes)

