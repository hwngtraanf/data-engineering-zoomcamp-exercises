import sys
import pandas as pd

print('arguments: ', sys.argv)

args = sys.argv
month = int(args[1])

df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
df['month'] = month
print(df.head())

df.to_parquet(f"output_month_{month}.parquet")


print(f'Hello Pipeline, {month}')
