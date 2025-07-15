import pandas as pd
df = pd.read_parquet('output.parquet')
print(df.iloc[10:20])
# print(df)