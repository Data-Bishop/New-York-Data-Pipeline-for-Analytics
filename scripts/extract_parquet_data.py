import pyarrow.parquet as pq
import requests
from io import BytesIO

def download_parquet_data(url, chunk_size=1024):

    response = requests.get(url, stream=True)

    if response.status_code == 200:
        # Create an empty BytesIO object to store the data
        parquet_data = BytesIO()
            
        for chunk in response.iter_content(chunk_size=chunk_size):
            parquet_data.write(chunk) # Write to the BytesIO object
        
        return parquet_data
        
    else:
        print(f"failed to retrieve data. Status code: {response.status_code}")
        return None
    
def read_parquet_from_bytesio(parquet_data):
    # Reset the position to the beginning
    parquet_data.seek(0)
    
    # Read the parquet data from the BytesIO object
    table = pq.read_table(parquet_data)
    return table

def main():
    # URL of the parquet file
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet'
    
    # Download the parquet data
    parquet_data = download_parquet_data(url)
    
    if parquet_data:
        # Read the parquet data into a PyArrow table
        table = read_parquet_from_bytesio(parquet_data)
        
        if table:
            # Display the schema of the PyArrow Table
            print(table.schema)
        else:
            print("Failed to read the parquet data.")
            
        # Close the BytesIO object
        parquet_data.close()

if __name__ == '__main__':
    main()      