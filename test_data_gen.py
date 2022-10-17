import pandas as pd

df = pd.read_csv('alliant_test_data/sebbo-test.csv')
df.to_parquet('alliant_test_data/parquet/seb-duplicate-test-01.parquet')
