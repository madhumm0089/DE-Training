import pandas as pd
import logging

logging.basicConfig(
    filename='parking_etl.log',
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] %(message)s'
    )

def load_csv(input_csv):
    try:
        df = pd.read_csv(input_csv)
        return df
    except FileNotFoundError:
        print(f"[Error] File not found: {input_csv}")

def clean_data(df):
    #  print(df)
     df = df.ffill()
     return df

def validate_data(df):
    logging.info('validating entry and exit time')
    if df['vehicle_id'].isnull().any():
        raise ValueError('[Error] Missing vehicle Id')
     
    try:
        df['entry_time'] = pd.to_datetime(df['entry_time'], errors='raise')
        df['exit_time'] = pd.to_datetime(df['exit_time'], errors='raise')
    except Exception as e:
        raise ValueError(f"[Error] Invalid datetime format: {e}")
    
    invalid_rows = df[df['entry_time'] > df['exit_time']]
    if not invalid_rows.empty:
        print(invalid_rows)  # Only print the invalid rows
        raise ValueError("[Error] Invalid entry: entry_time is after exit_time.")

    return df
        


def csv_to_parquet(df, output_file):
    logging.info(f"Writing csv file to parquet: {output_file}")
    df.to_parquet(output_file, index=False)
    logging.info("Conversion completed")
    return True

def ETL(input, output):
    logging.info('Starting ETL process:')
    df = load_csv(input)

    df_clean = clean_data(df)
    
    logging.info("Writing to parquet")
    csv_to_parquet(df_clean, output)
    logging.info('reading parquet file')
    parquet_df=pd.read_parquet(output)
    print(parquet_df)
    return parquet_df

