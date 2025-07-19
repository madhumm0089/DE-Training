import pandas as pd
from airport_parking_toolkit import extract, clean_data, validate, write
import logging
import argparse

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
    parser = argparse.ArgumentParser(description="Covert csv to parquet")
    parser.add_argument("--input", required=True, help='input csv file')
    parser.add_argument('--outfile', required=True, help='output file path')
    args = parser.parse_args()

    

    df = extract.load_csv(args.input)
    df_clean = clean_data(df)
    df_validate = validate.validate_data(df_clean)
    write.csv_to_parquet(df_clean, args.outfile)
    print(pd.read_parquet('output.parquet'))


if __name__ == '__main__':
    main()
