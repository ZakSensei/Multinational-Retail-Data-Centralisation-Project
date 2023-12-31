from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from pandasgui import show
import sqlalchemy
import yaml

class DatabaseConnector():
    """This is a class that will contain methods to connect us with and upload data to the database"""

    def __init__(self, credential_file_path="db_creds.yaml"):
        self.cred_dict = self.read_db_creds(credential_file_path)

    def read_db_creds(self, yaml_file_path):
        """
        Reads the credentials YAML file and return a dictionary of the credentials.

        Parameters:
        - yaml_file_path (str): Path to the YAML file containing database credentials.

        Returns:
        - dict: Dictionary containing database credentials.
        """
        with open(yaml_file_path, "r") as file:
            credentials = yaml.safe_load(file)
            return credentials

    def init_db_engine(self):
        """
        Initialize and return an SQLAlchemy database engine using credentials from the YAML file.

        Returns:
        - sqlalchemy.engine.base.Engine: SQLAlchemy database engine.
        """
        connection = f"postgresql://{self.cred_dict['DB_USER']}:{self.cred_dict['DB_PASSWORD']}" + \
                     f"@{self.cred_dict['DB_HOST']}:{self.cred_dict['DB_PORT']}" + f"/{self.cred_dict['DB_DATABASE']}"

        engine = sqlalchemy.create_engine(connection)
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        return engine

    def list_db_tables(self):
        """
        List the names of tables in the connected database.

        Returns:
        - list: List of table names in the connected database.
        """
        engine = self.init_db_engine()
        with engine.connect() as connection:
            inspector = sqlalchemy.inspect(engine)
            table_names = inspector.get_table_names()
            return table_names

    def upload_to_db(self, dataframe, table_name):
        """
        Upload a DataFrame to the connected database.

        Parameters:
        - dataframe (pandas.DataFrame): DataFrame to be uploaded.
        - table_name (str): Name of the table in the database.

        Returns:
        - None
        """
        localdb_conn = DatabaseConnector("localdb_creds.yaml")
        engine = localdb_conn.init_db_engine()
        dataframe.to_sql(table_name, engine, index=False, if_exists='replace')

if __name__ == "__main__":
    # Create instances of classes
    db_ext = DataExtractor()
    db_clean = DataCleaning()
    db_conn = DatabaseConnector()

    # Create database engine
    engine = db_conn.init_db_engine()

    #Components connecting to API
    number_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    api_headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    retrieve_store_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"

    # Extract card details and convert to a DataFrame (df)
    card_details_df = db_ext.retrieve_pdf_data()

    # Extract data from the "legacy_users" table and convert to a DataFrame
    users_df = db_ext.read_rds_table("legacy_users", engine)

    # Convert the card_details_df DataFrame to a CSV file 'mrdc.csv'
    card_details_df.to_csv("mrdc.csv", index=False)

    # Cleans the DataFrames
    users_df = db_clean.clean_user_data(users_df)
    card_details_df = db_clean.clean_card_data(card_details_df)

    # Upload the cleaned DataFrames to the sales_data Database
    db_conn.upload_to_db(users_df, 'dim_users')
    db_conn.upload_to_db(card_details_df, 'dim_card_details')

    # Print table names in rds databases
    print(f"{db_conn.list_db_tables()} \n")

    # Show a graphical user interface (GUI) of the specified DataFrame
    show(card_details_df)

    #Components connecting to API
    number_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    api_headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    retrieve_store_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"

    number_of_stores = db_ext.list_number_of_stores(number_of_stores_endpoint, api_headers)
    store_df = db_ext.retrieve_stores_data(retrieve_store_endpoint, number_of_stores, api_headers)


    store_df.to_csv("mrdc.csv", index=False) 
    store_df = db_clean.clean_store_data(store_df)
    #show(store_df)
