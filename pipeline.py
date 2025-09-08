import os
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
import logging

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
        rows_to_insert = [
            {'currency_id': 'bitcoin', 'price_usd': data.get('bitcoin', {}).get('usd')},
            {'currency_id': 'ethereum', 'price_usd': data.get('ethereum', {}).get('usd')}
        ]
        return rows_to_insert
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        return None
    
def load_to_bigquery(rows, table_id):
    full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
    table_ref = client.dataset(DATASET_ID).table(table_id)
    schema = [
        bigquery.SchemaField('currency_id', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('price_usd', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('ingestion_time', 'TIMESTAMP', mode='NULLABLE')
    ]

    try:
        client.get_table(table_ref)
        logging.info(f"Table {full_table_id} already exists.")
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        logging.info(f"Table {full_table_id} created.")

    ingestion_time = bigquery.job.QueryJob(query='SELECT CURRENT_TIMESTAMP()').result().to_dataframe().iloc[0, 0]

    for row in rows:
        row['ingestion_time'] = ingestion_time
    logging.info(f"Loading {len(rows)} rows into {full_table_id}")
    errors = client.insert_rows_json(table_ref, rows)

    if not errors:
        logging.info("Data loaded successfully.")
    else:
        logging.error(f"Encountered errors while inserting rows: {errors}")
        raise RuntimeError("BigQuery load failed.")
    
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
    api_endpoint = 'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd'
    
    try:
        raw_data = fetch_api_data(api_endpoint)
        if raw_data:
            load_to_bigquery(raw_data, RAW_TABLE_ID)
            transform_data(TRANSFORMATION_SQL_PATH)
        else:
            logging.warning("Pipeline failed. No data to process.")
    except Exception as e:
        logging.critical(f"Pipeline terminated due to a critical error: {e}")