import pandas as pd
from airport_parking_toolkit.cli_tool import ETL


def test_etl(tmp_path):
    input_csv = tmp_path / "input.csv"
    output_parquet = tmp_path / "output.parquet"

    input_csv.write_text(
        "vehicle_id,zone_id,entry_time,exit_time,paid_amount\n"
        "V001,Z001,2024-07-18 08:00:00,2024-07-18 10:30:00,120.0\n"
        "V002,Z002,2024-07-18 09:00:00,2024-07-18 11:00:00,80.0\n"
    )

    result_df = ETL(str(input_csv), str(output_parquet))

    assert result_df['paid_amount'].sum() == 200
    assert len(result_df) == 2
