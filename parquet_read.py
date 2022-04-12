import pandas as pd
df = pd.read_parquet("/Users/dishamukherjee/Desktop/pragma/output.parquet",engine="pyarrow")
print(df.head())
