import argparse
from .cli_tool import ETL 

def main():
    parser = argparse.ArgumentParser(description='Convert CSV to Parquet')
    parser.add_argument('--infile', required=True, help='Input CSV path')
    parser.add_argument('--outfile', required=True, help='Output Parquet path')
    args = parser.parse_args()
    ETL(args.infile, args.outfile)



