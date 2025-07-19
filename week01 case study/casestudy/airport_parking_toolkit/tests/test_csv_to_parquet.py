import pandas as pd
from airport_parking_toolkit.cli_tool import csv_to_parquet

def test_csv_to_parquet(tmp_path):
    df = pd.DataFrame({
        'vehicle_id': ['V001'],
        'entry_time': ['2025-07-19 08:00:00'],
        'exit_time': ['2025-07-19 09:00:00']
    })

    output_file = tmp_path / "output.parquet"

    success = csv_to_parquet(df, str(output_file))

    assert success
    assert output_file.exists()
