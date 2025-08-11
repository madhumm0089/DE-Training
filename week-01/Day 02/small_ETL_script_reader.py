import pandas as pd
import argparse

parser =argparse.ArgumentParser()
parser.add_argument('dept', type=str)
args= parser.parse_args()


df = pd.read_csv("employee.csv").fillna(0)
# print(df.columns.tolist())
print('filtered rows:')
filteredEmployee = df[df['Department']==args.dept]
print(filteredEmployee)


filteredEmployee.to_csv('enginering_employee.csv', index=True)