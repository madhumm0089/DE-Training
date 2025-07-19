import logging
import pandas as pd

def csv_to_parquet(df, output_file):
    logging.info(f"Writing csv file to parquet: {output_file}")
    df.to_parquet(output_file, index=False)
    logging.info("Conversion completed")
    

