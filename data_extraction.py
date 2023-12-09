import pandas as pd
import tabula as tb

class DataExtractor():
    """This is a utility class which helps extract data from different data sources including: 
        CSV files, an API and an S3 bucket."""

    def read_rds_table(self, table_name, engine):
        df = pd.read_sql_table(table_name, engine)
        df.drop("index", axis= "columns", inplace=True)
        return df

    def retrieve_pdf_data(self, link = "card_details.pdf"):
        card_details_csv = tb.read_pdf(link, multiple_tables=True, pages='all', lattice=True)
        df = pd.concat(card_details_csv)
        df.reset_index(inplace=True)
        df.drop("index", axis= "columns", inplace=True)
        return df