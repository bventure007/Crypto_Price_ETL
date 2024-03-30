# Crypto_Price_ETL
This project is about cryptocurrencies price data extraction in real time

It involves building of an ETL pipeline that pulls crytocorrencies data from coinmarket cap using RAPID API.

The pulled data from rapid API involves staging it in S3 Bucket (data lake) and transformed into a data warehouse (redshift).

The codes were divided into different modules

etl.py
util.py
main.py
.env files


# Crypto Price ETL Script

## Overview
The Crypto Price ETL (Extract, Transform, Load) script is designed to  gather raw cryptocurrency price data from a rapid api, transforming it into a structured format, and loading it into a redshift for further analysis or storage. This script streamlines the data processing workflow, making it easier to work with cryptocurrency price data for various purposes such as financial analysis, forecasting, or research.

## Prerequisites

Before running the script, ensure you have the following:
- **Python 3**: The script is written in Python 3, so ensure you have Python 3 installed on your system.
- **Required Packages**: Install the necessary Python packages by running the following command:
  ```
  pip install -r requirements.txt
  ```
or import the below libraries

# import libraries
from util import get_redshift_connection,\
execute_sql, list_files_in_folder
import pandas as pd
import requests
import boto3
from datetime import datetime
from io import StringIO
import io
import psycopg2
import ast
from dotenv import dotenv_values


## Usage

By default, the script will perform the following steps:
- Extract cryptocurrency price data from rapid api.
- Transform the raw data into a structured format based on the configured options.
- Load the transformed data into the default target destination (redshift).

## Configuration

The `config.json` file contains configuration options for the script. Customize the following parameters according to your requirements:
- **Source**: Specify the source from which to extract cryptocurrency price data (API Sources).
- **Target**: Define the target destination where the transformed data will be loaded (Amazon Redshift).
- **Options**: You can include additional options or parameters for the ETL process (Orchestration using airflow).

## Contributing

Contributions to the Crypto Price ETL script are welcome! If you encounter any issues, have suggestions for improvements, or would like to add new features, feel free to open an issue or submit a pull request on GitHub.

