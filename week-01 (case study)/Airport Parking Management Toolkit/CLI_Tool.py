import pandas as pd
import argparse
import logging
import csv

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def process_parking_events():
    df = pd.read_csv('parking_events.csv')
    return df

def clean_validate(df):
    df = df.dropna(subset=['vehicle_id','entry_time','exit_time'])
    print(df)

if __name__ == '__main__':
    clean_validate(df=pd.read_csv('parking_events.csv'))
