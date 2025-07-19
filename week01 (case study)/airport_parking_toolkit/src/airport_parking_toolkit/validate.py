def validate_data(df):
    invalid_rows = df[df['entry_time'] > df['exit_time']]
    if not invalid_rows.empty:
        print('Invalid entry')

    return df[df['entry_time'] > df['exit_time']]