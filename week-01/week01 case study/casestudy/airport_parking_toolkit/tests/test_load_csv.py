import pandas as pd
from airport_parking_toolkit.cli_tool import load_csv

def test_load_csv_file_not_found():
    df = load_csv('file.csv')
    assert df is None