import pandas as pd
from airport_parking_toolkit.cli_tool import clean_data

def test_clean_data_ffill():
    df = pd.DataFrame({
        'vehicle_id': ['V001', None],
        'entry_time': ['2025-07-19 08:00:00', None],
        'exit_time': ['2025-07-19 09:00:00', None]
    })
    cleaned = clean_data(df)
    assert cleaned.isnull().sum().sum() == 0