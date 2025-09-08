Data Engineering Pipeline: Cryptocurrency Analytics

This project demonstrates an end-to-end data engineering pipeline built using Python and Google BigQuery. The pipeline automates the process of extracting data from a public API, loading it into a cloud data warehouse, and transforming it for analysis. This is a practical example of a modern Extract, Load, Transform (ELT) workflow.

1. Project Overview

The goal of this project is to create an automated pipeline that fetches cryptocurrency price data from the CoinGecko API, loads the raw data into Google BigQuery, and then performs advanced transformations to create a clean, analytics-ready dataset. The final output can be used for building dashboards or reports to monitor market trends.

The pipeline is designed to be:

Automated: The entire process is orchestrated by a single Python script.

Scalable: Leverages BigQuery's serverless architecture to handle growing datasets efficiently.

Robust: Includes logging, error handling, and idempotent operations to ensure reliability.

2. Key Features

API Data Ingestion: Fetches real-time cryptocurrency data using the requests library.

Cloud Data Loading: Loads raw data directly into a BigQuery staging table.

Advanced Data Transformation: Uses SQL to clean data, calculate metrics (e.g., price change percentage), and prepare it for analysis using a window function and self-join.

Modular Design: Separates code from configuration and SQL queries, making the project easy to maintain and extend.

Secure Authentication: Authenticates with GCP using a service account key, a standard practice for production environments.

3. Technology Stack

Programming Language: Python 3.9+
Cloud Platform: Google Cloud Platform (GCP)
BigQuery: Serverless data warehouse for storage and transformation.

APIs & Libraries:

CoinGecko API: Source of cryptocurrency price data.
google-cloud-bigquery: Python client for interacting with BigQuery.
requests: Python library for making API calls.

Version Control: Git

4. Project Structure

Crypto Pipeline/
├── .gitignore
├── pipeline.py
├── transform_data.sql
├── requirements.txt
└── README.md

pipeline.py: The main Python script that orchestrates the ELT process.

transform_data.sql: Contains the SQL query for transforming raw data into a clean, final table.

.gitignore: Specifies files to be ignored by Git (e.g., credentials, log files).

requirements.txt: Contains the required dependecies needed for execution.

README.md: This documentation file.

5. Setup and Installation

Prerequisites:-

Google Cloud Project: A GCP account with a project created and billing enabled.

APIs: Enable the BigQuery API and Cloud Storage API in your GCP project.

Service Account: Create a service account with the BigQuery Data Editor and BigQuery Job User roles. Download the JSON key file and save it securely.

BigQuery Dataset: Manually create a BigQuery dataset (e.g., crypto_data_pipeline) in the GCP console.

Local Setup:-

Clone the Repository using the below commands:

git clone https://github.com/your-username/your-repo-name.git
cd crypto_pipeline

Install Dependencies using the below commands:

pip install -r requirements.txt

Configure Script:

Open pipeline.py.
Update the PROJECT_ID, DATASET_ID, and KEY_FILE_PATH variables with your specific information.

6. Usage

To run the entire pipeline, simply execute the main Python script from your terminal:

python pipeline.py

The script will:

Fetch data from the CoinGecko API.
Load it into the raw_crypto_prices table in your BigQuery dataset.
Execute the transform_data.sql query to create or replace the clean_crypto_data table.

7. Future Enhancements

Automation: Schedule the pipeline using GCP Cloud Scheduler or Apache Airflow to run at regular intervals.

Notifications: Add a feature to send email or Slack notifications on pipeline success or failure.

Monitoring: Integrate with a monitoring service like GCP Cloud Monitoring to track pipeline performance and resource usage.

Parameterized Queries: Modify the pipeline to fetch a configurable list of cryptocurrencies instead of hardcoding them.

Reporting : Integrate with looker studio for creating Dashboards for Data Visualization.
