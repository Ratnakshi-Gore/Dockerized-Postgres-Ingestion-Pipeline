#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm

get_ipython().system('uv add tqdm')
get_ipython().system('uv add ipywidgets jupyterlab_widgets')
get_ipython().system('uv add sqlalchemy')
get_ipython().system('uv add psycopg2-binary')


pd.__file__


taxi = pd.read_csv('/yellow/yellow_tripdata_2021-06.csv.gz')



dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]



taxi = pd.read_csv('https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-06.csv.gz',
                   dtype=dtype, 
                   parse_dates=parse_dates)


def run():
    pg_user = 'root'
    pg_pass = 'root'
    pg_host = 'localhost'
    pg_db = 'ny_taxi'
    pg_port = 5433
    year = 2021
    month = 1
    chunksize=100000



prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download'
url = f'{prefix}/yellow/yellow_tripdata_{year}-{month:02d}.csv.gz'

engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')


taxi.head(0).to_sql(name='yellow_taxi_data',con=engine, if_exists='replace')


print(pd.io.sql.get_schema(taxi, name='yellow_taxi-data',con=engine))



df_iter = pd.read_csv(
    'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-06.csv.gz',
    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=chunksize,
    )



for taxi in df_iter:
    taxi.head(0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')
    print(len(taxi))



df_iter = pd.read_csv(
    url,
    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=chunksize,
    )

first = True

for taxi_chunk in tqdm(df_iter):
    if first:
        taxi_chunk.head(0).to_sql(
            name=target_table,
            con=engine,
            if_exists='replace'
        )
first = False

taxi_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists='append'
        )


if __name__ == '__main__':
    run()



