def clean_data(df):
     df = df.dropna(subset=['vehicle_id','entry_time','exit_time'])
     return df