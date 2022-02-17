# get season and episode

import pandas as pd

prod_df = pd.read_pickle("long_series_producers_s3.pkl")

print(len(prod_df))