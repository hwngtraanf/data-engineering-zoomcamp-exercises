import pandas as pd
import psycopg2
import click
import os

from sqlalchemy import create_engine
from tqdm.auto import tqdm
from dotenv import load_dotenv


@click.command()
@click.option('--pg_host', default='localhost', help='PostgreSQL host')
@click.option('--year', default=2021, type=int, help='Year')
@click.option('--month', default=1, type=int, help='Month')
@click.option('--chunk_size', default=100000, type=int, help='Chunk size to ingest data')
@click.option('--target_table', default='yellow_taxi_data', help='Target table name')
def run(pg_host, year, month, chunk_size, target_table):

    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'

    load_dotenv()
    PG_USER = os.getenv('PG_USER')
    PG_PASSWORD = os.getenv('PG_PASSWORD')
    PG_HOST = pg_host
    PG_PORT = os.getenv('PG_PORT')
    PG_DATABASE = os.getenv('PG_DATABASE')

    engine = create_engine(f'postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}')
    conn = psycopg2.connect(database=PG_DATABASE, user=PG_USER, password=PG_PASSWORD, host=PG_HOST, port=PG_PORT)
    cur = conn.cursor()
    cur.execute(f"SELECT 1 FROM information_schema.tables WHERE table_name = '{target_table}';")

    is_created = cur.fetchone()

    # config for reading csv file
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

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunk_size,
    )

    # # get schema
    # print(pd.io.sql.get_schema(next(df_iter), name='yellow_taxi_data', con=engine))

    if not is_created:
        # create table
        next(df_iter).head(0).to_sql(name=target_table, con=engine, if_exists='replace')
        print(f"Table {target_table} created")
    # ingest data in chunk
    for df_chunk in tqdm(df_iter):
        df_chunk.to_sql(name=target_table, con=engine, if_exists='append')
        print(f"Inserted {len(df_chunk)} rows")

if __name__ == "__main__":
    run()