import pandas as pd

class DataExtractor():
    """This is a utility class which helps extract data from different data sources including: 
        CSV files, an API and an S3 bucket."""

    def read_rds_table(self, table_name, engine):
        df = pd.read_sql_table(table_name, engine)
        return df