import pandas as pd
import tabula

class DataExtractor():
    """This is a utility class which helps extract data from different data sources including: 
        CSV files, an API and an S3 bucket."""

    def read_rds_table(self, table_name, engine):
        df = pd.read_sql_table(table_name, engine)
        return df

    def retrieve_pdf_data(self, link = "card_details.pdf"):
        card_details_csv = tabula.convert_into(link, "card_details.csv", output_format="csv", pages='all')
        df = pd.read_csv(card_details_csv)
        return df