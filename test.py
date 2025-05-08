import os
import pandas as pd

for file in os.listdir('./data'):
    if 'sales' not in file:
        df = pd.read_csv(os.path.join('./data', file))
        print(f"{file}")
        print(len(df))
        print(df.columns)
        print()
        print()