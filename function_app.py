import azure.functions as func
import logging
import pyarrow as pa
import pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from io import BytesIO
from datafile import summarize_data
import pandas as pd

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# Azure Blob Storage Configuration
CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=devnewsweekadls;AccountKey=27yFIzheCgRy+FpbtUAGmfm6tRuhgxyXjRsAiz7VuY4BGNpQcVfbJ7etekwG29dchJsIQ4Mb0jTr+AStYbuHxg==;EndpointSuffix=core.windows.net'"
CONTAINER_NAME = "bronze"  # Azure Blob Storage container name
BLOB_NAME = "topline_kpis/data.parquet"  # Blob name to store processed data

def save_dataframe_to_azure(data, container_name, blob_name):
    
    try:
        # Connect to Azure Blob Storage
        blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(container_name)

        # Convert DataFrame to Parquet format
        table = pa.Table.from_pandas(data)
        parquet_buffer = BytesIO()
        pq.write_table(table, parquet_buffer, compression='snappy')
        parquet_buffer.seek(0)

        # Upload Parquet file to Azure Blob Storage
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(parquet_buffer, overwrite=True)

        logging.info(f"Successfully saved {blob_name} to Azure Blob Storage.")
        return True

    except Exception as e:
        logging.error(f"Error saving DataFrame to Azure: {e}")
        return False


@app.route(route="http_trigger0", auth_level=func.AuthLevel.FUNCTION)
def http_trigger0(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    
    try:
        # Connect to Azure Blob Storage
        blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(CONTAINER_NAME, BLOB_NAME)

        # Download and read the dataset from Azure Blob Storage
        stream = BytesIO(blob_client.download_blob().readall())  # Load the file into memory
        data = pd.read_parquet(stream)  # Read as DataFrame

        # Process dataset using summarize_data function
        summarize_data(data)

        # Save the processed dataset back to Azure Blob Storage
        if save_dataframe_to_azure(data, CONTAINER_NAME, BLOB_NAME):
            return func.HttpResponse("Success", status_code=200)
        else:
            return func.HttpResponse("Problem saving data", status_code=500)

    except Exception as e:
        logging.error(f"Error in HTTP trigger function: {e}")
        return func.HttpResponse(f"Internal server error: {e}", status_code=500)