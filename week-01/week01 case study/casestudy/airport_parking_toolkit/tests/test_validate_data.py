import pytest
import pandas as pd
from airport_parking_toolkit.cli_tool import validate_data

@pytest.mark.parametrize(
    "vehicle_id, entry_time, exit_time, should_raise, expected_error",
    [
        ('V001', '2025-07-32 08:00:00', '2025-07-19 09:00:00', True, 'Invalid datetime format'),
        (None, '2025-07-19 08:00:00', '2025-07-19 09:00:00', True, 'Missing vehicle Id'),
        ('V002', '2025-07-19 08:00:00', '2025-07-19 07:00:00', True, 'entry_time is after exit_time'),
        ('V003', '2025-07-19 08:00:00', '2025-07-19 09:00:00', False, None),
    ]
)
def test_time_validation(vehicle_id, entry_time, exit_time, should_raise, expected_error):
    df = pd.DataFrame({
        'vehicle_id': [vehicle_id],
        'entry_time': [entry_time],
        'exit_time': [exit_time]
    })

    if should_raise:
        with pytest.raises(ValueError) as exc_info:
            validate_data(df)
        assert expected_error in str(exc_info.value)
    else:
        result = validate_data(df)
        assert isinstance(result, pd.DataFrame)
