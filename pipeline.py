import os
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core import exceptions
import logging
from datetime import datetime
import time

PROJECT_ID = 'core-onelms-poc'
DATASET_ID = 'crypto_data_pipeline'
RAW_TABLE_ID = 'raw_crypto_prices'
CLEAN_TABLE_ID = 'clean_crypto_data'
KEY_FILE_PATH = r"C:\Users\50084549\Desktop\All Folders\BQ KEY\bigquery-service@core-onelms-poc.iam.gserviceaccount.com.json"
TRANSFORMATION_SQL_PATH = "transform_data.sql"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)

try:
    credentials = service_account.Credentials.from_service_account_file(KEY_FILE_PATH)
    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
    logging.info("Authentication successful using service account key file.")
except FileNotFoundError as e:
    logging.error(f"Service account key file not found: {KEY_FILE_PATH}. Exiting.")
    exit()
except Exception as e:
    logging.error(f"An unexpected error occurred during authentication: {e}")
    exit()

def fetch_api_data(api_url):
    logging.info(f"Fetching data from API: {api_url}")
    try:
        response = requests.get(api_url)
        response.raise_for_status() 
        data = response.json()
        logging.info("Data fetched successfully.")
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        return None

def load_to_bigquery(raw_data):
    logging.info("Starting data load to BigQuery...")
    dataset_ref = client.dataset(DATASET_ID)
    table_ref = dataset_ref.table(RAW_TABLE_ID)

    try:
        client.get_dataset(dataset_ref)
        logging.info(f"Dataset {PROJECT_ID}.{DATASET_ID} already exists.")
    except exceptions.NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        client.create_dataset(dataset)
        logging.info(f"Dataset {PROJECT_ID}.{DATASET_ID} created.")
    except Exception as e:
        logging.error(f"An error occurred while creating or checking for dataset existence: {e}")
        raise RuntimeError("BigQuery dataset operation failed.")

    try:
        client.get_table(table_ref)
        logging.info(f"Table {PROJECT_ID}.{DATASET_ID}.{RAW_TABLE_ID} already exists.")
    except exceptions.NotFound:
        # Define the schema for the table
        schema = [
            bigquery.SchemaField("currency_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("price_usd", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("market_cap_usd", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("vol_24hr_usd", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("change_24hr_usd", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("ingestion_time", "TIMESTAMP", mode="NULLABLE"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        
        try:
            client.create_table(table)
            logging.info(f"Table {PROJECT_ID}.{DATASET_ID}.{RAW_TABLE_ID} created.")
        except exceptions.Conflict:
            logging.warning(f"Table {PROJECT_ID}.{DATASET_ID}.{RAW_TABLE_ID} already existed but wasn't found on the first check.")
    except Exception as e:
        logging.error(f"An error occurred while creating or checking for table existence: {e}")
        raise RuntimeError("BigQuery table operation failed.")
        
    rows_to_insert = []
    ingestion_time = datetime.now()
    
    for currency_id, prices in raw_data.items():
        row = {
            "currency_id": currency_id,
            "price_usd": prices.get("usd"),
            "market_cap_usd": prices.get("usd_market_cap"),
            "vol_24hr_usd": prices.get("usd_24h_vol"),
            "change_24hr_usd": prices.get("usd_24h_change"),
            "ingestion_time": ingestion_time.isoformat()
        }
        rows_to_insert.append(row)
    
    if rows_to_insert:
        try:
            errors = client.insert_rows_json(table_ref, rows_to_insert)
            if errors:
                logging.error(f"Encountered errors while inserting rows: {errors}")
                raise RuntimeError("BigQuery insertion failed.")
            else:
                logging.info(f"{len(rows_to_insert)} rows successfully loaded to BigQuery table '{RAW_TABLE_ID}'.")
        except Exception as e:
            logging.error(f"An error occurred during BigQuery load: {e}")
            raise RuntimeError("BigQuery insertion failed.")

def transform_data(sql_file_path):
    logging.info("Starting data transformation...")

    try:
        with open(sql_file_path, 'r') as file:
            transform_query = file.read()
    except FileNotFoundError:
        logging.error(f"Error: SQL file not found at {sql_file_path}")
        return
    
    transform_query = transform_query.replace('core-onelms-poc', PROJECT_ID)
    transform_query = transform_query.replace('crypto_data_pipeline', DATASET_ID)
    transform_query = transform_query.replace('raw_crypto_prices', RAW_TABLE_ID)
    transform_query = transform_query.replace('clean_crypto_data', CLEAN_TABLE_ID)

    try:
        query_job = client.query(transform_query)
        query_job.result()  
        logging.info(f"Data transformation complete. Clean table '{CLEAN_TABLE_ID}' created.")
    except Exception as e:
        logging.error(f"An error occurred during transformation: {e}")
        raise RuntimeError("BigQuery transformation failed.")
    
if __name__ == "__main__":
    cryptocurrency_ids = [
        "bitcoin", "ethereum", "tether", "binancecoin", "solana", "xrp", 
        "cardano", "dogecoin", "shiba-inu", "avalanche-2", "polkadot", 
        "litecoin", "chainlink", "tron", "polygon", "bitcoin-cash", "uniswap", 
        "wrapped-bitcoin", "stellar", "monero", "ethereum-classic", "cosmos", 
        "filecoin", "internet-computer", "optimism"
    ]
    
    ids_string = ",".join(cryptocurrency_ids)

    api_endpoint = (
        f'https://api.coingecko.com/api/v3/simple/price?ids={ids_string}'
        '&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true'
        '&include_24hr_change=true'
    )
    
    try:
        # Step 1: Fetch data from the API
        raw_data = fetch_api_data(api_endpoint)
        if not raw_data:
            logging.critical("Pipeline failed. No data to process.")
            exit()

        # Step 2: Load data into BigQuery
        load_to_bigquery(raw_data)
        time.sleep(10)  # Wait for data to be available for the next transformation

        # Step 3: Transform the data in BigQuery
        transform_data(TRANSFORMATION_SQL_PATH)

        logging.info("Pipeline executed successfully.")

    except RuntimeError as e:
        # Catch exceptions raised by the load and transform functions
        logging.critical(f"Pipeline execution failed: {e}")