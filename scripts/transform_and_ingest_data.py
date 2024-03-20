import pandas as pd
from sqlalchemy import create_engine, text
import pyarrow.parquet as pq
from io import BytesIO
from test_parquet import read_parquet_file
from extract_parquet_data import download_parquet_data, read_parquet_from_bytesio


def load_data_into_postgres(df, batch_size=1000):
    
    connection_string = 'postgresql://postgres:Databishop@localhost:5432/legodb'
    engine =  create_engine(connection_string)

    # Create the database table
    table_name = "yellow_taxi"
    
    try:
        with engine.connect() as conn:
            for i in range(0, len(df), batch_size):
                batch_df = df[i:i+batch_size]
                batch_df.to_sql(table_name, conn, if_exists='append', index=False)
                print(f"Inserted {len(batch_df)} records into {table_name} (Batch {i//batch_size + 1})")
        print(f"The {table_name} created successfully.")
    except Exception as e:
        print(f'Error creating table and inserting data ({table_name}): {e}')

def main():
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"

    parquet_data = download_parquet_data(url)

    if parquet_data:
        table = read_parquet_from_bytesio(parquet_data)

        if table:
            # Convert PyArrow Table to Pandas DataFrame
            df = table.to_pandas()
            
            # Display the transformed DataFrame
            print(df.head())

            # Load data into PostgreSQL database
            load_data_into_postgres(df)

            # Close the BytesIO object
            parquet_data.close()
        else:
            print("Failed to read Parquet data.")
    else:
        print("Failed to download Parquet data.")

if __name__ == "__main__":
    main()