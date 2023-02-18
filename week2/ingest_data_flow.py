#!/usr/bin/env python
# coding: utf-8

import os 
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    # url = params.url


    csv_name = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

    # download the csv
    #os.system(f"wget {url} -O {csv_name}")
    #pd.read_csv({url})

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')
    
    for item in df_iter:
        t_start = time()
            
        item.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        item.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        item.to_sql(name=table_name, con=engine, if_exists='append')
        
        t_end = time()
        
        print('inserted another chunk..., took %.3f second' % (t_end - t_start))




if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    # user, password, host, port, database name, table name, URL of the csv

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='db name for postgres')
    parser.add_argument('--table_name', help='name of the table where results are written')
    # parser.add_argument('--url', help='url of the csv file')


    args = parser.parse_args()
 

    main(args)




    




