import sys
import pandas as pd

# we have whole year's data and want to process month no. 12
# hence we will parameterise it, so we can run commands like python pipeline 12
print('hello pipeline')

print ('arguments', sys.argv[1])

month = int(sys.argv[1])   # we can pass on argument ofr month in the 2nd position as 1st will always be the filename

df = pd.DataFrame({'day':[1,2], 'num_passengers':[3,4]})
df['month']= month
print(df.head())

df.to_parquet(f'ouput_{month}.parquet')

print(f'hello pipeline, month={month}')























