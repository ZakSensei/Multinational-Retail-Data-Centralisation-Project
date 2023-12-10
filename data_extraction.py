import pandas as pd
import tabula as tb

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
