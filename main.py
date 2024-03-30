
# Import libraries
from time import sleep
from util import generate_schema, execute_sql, transform_data, get_redshift_connection, empty_raw_folder
from etl import get_data_from_api, read_from_s3, write_to_s3, load_to_redshift, read_multi_files_from_s3, \
move_files_to_processed_folder

conn = get_redshift_connection()
# Main method to run the pipeline
def main():
    bucket_name = 'crypto-price-etl-data'
    raw_data_folder = 'raw_data'
    processed_data_folder = 'processed_data'
    transformed_data_folder = 'transformed_data'
    redshift_table_name = 'crypto_price_data'
    counter = 1
    
    # A while loop to send 5 requests to the API
    while counter <= 5:
        print('Pulling data from API...')
        crypto_price_data = get_data_from_api() # Extract data from API
        print('Writing data to S3...')
        write_to_s3(crypto_price_data, bucket_name, raw_data_folder)
        counter+= 1
        sleep(5) # Wait 50 seconds before sending another request to the API
    print('All API data pulled and written to S3 bucket')

    #Transform the data
    raw_data = read_multi_files_from_s3(bucket_name, raw_data_folder)
    transformed_data = transform_data(raw_data) # Read data from S3 and transform
    # write transformed data to S3
    print('Writing transformed data to transformed folder in S3')
    write_to_s3(transformed_data, bucket_name, transformed_data_folder)
    create_table_query = generate_schema(transformed_data, redshift_table_name) # generate ddl of target table
    #print(create_table_query)
    execute_sql(create_table_query, conn) # Create a taget table for in Redshift
    print('Schema/Table created in Redshift')
    load_to_redshift(bucket_name, transformed_data_folder, redshift_table_name)
    #Prepare the raw data folder for recieving new data
    move_files_to_processed_folder(bucket_name, raw_data_folder, processed_data_folder)
    # You may also choose to empty the raw folder using below code:
    # empty_raw_folder(bucket_name, raw_data_folder)
    
main()

