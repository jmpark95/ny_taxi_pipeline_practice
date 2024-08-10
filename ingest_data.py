import pandas as pd
import argparse
from sqlalchemy import create_engine
from urllib.request import urlretrieve

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    filename = "yellow_taxi_data.parquet"

    urlretrieve(url, filename)

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    df = pd.read_parquet(filename, engine="auto")
    df.to_sql(name=table_name, con=engine, schema="public", if_exists="replace", chunksize=100000)

    print("Parquet successfully ingested into Postgres")


    # print(pd.io.sql.get_schema(df, name='yellow_taxi_data'))
    # chunks = pd.read_parquet('yellow_tripdata_2020-01.parquet', engine='auto', chunksize=100000)
    # for chunk in chunks:
    #     chunk.to_sql(name='yellow_taxi_data', con=engine, schema="public", if_exists="append")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest parquet data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='pw for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of table')
    parser.add_argument('--url', help='url of parquet')

    args = parser.parse_args()

    main(args)