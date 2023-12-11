import pandas as pd
import tabula as tb
import requests

class DataExtractor():
    """This is a utility class which helps extract data from different data sources including: 
        CSV files, an API, and an S3 bucket."""

    def read_rds_table(self, table_name, engine):
        """
        Read a table from a relational database and return it as a DataFrame.

        Parameters:
        - table_name (str): The name of the table to read from the database.
        - engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for the database.

        Returns:
        - pandas.DataFrame: The DataFrame containing the data from the specified table.
        """
        df = pd.read_sql_table(table_name, engine)
        df.drop("index", axis="columns", inplace=True)
        return df

    def retrieve_pdf_data(self, link="card_details.pdf"):
        """
        Retrieve data from a PDF file and return it as a DataFrame.

        Parameters:
        - link (str): The file path or URL of the PDF file. Defaults to "card_details.pdf".

        Returns:
        - pandas.DataFrame: The DataFrame containing the data extracted from the PDF file.
        """
        # Use tabula to read tables from the PDF
        card_details_csv = tb.read_pdf(link, multiple_tables=True, pages='all', lattice=True)
        
        # Concatenate tables into a single DataFrame
        df = pd.concat(card_details_csv)
        
        # Reset the index and drop the 'index' column
        df.reset_index(inplace=True)
        df.drop("index", axis="columns", inplace=True)
        
        return df

    def list_number_of_stores(self, endpoint, header):      
        """
        Retrieve the number of stores using the given API endpoint and headers.

        Parameters:
        - endpoint (str): The API endpoint for retrieving the number of stores.
        - headers (dict): The dictionary containing the headers for the API request.

        Returns:
        - int: The number of stores.
        """
        response = requests.get(endpoint, headers=header)
        
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
        # Access the response data as JSON
            number_of_stores = response.json()

        # If the request was not successful, print the status code and response text
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(f"Response Text: {response.text}")
        
        return number_of_stores

