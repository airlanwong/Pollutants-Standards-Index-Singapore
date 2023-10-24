import requests
import json
import pandas as pd
from datetime import datetime
import os
import logging
import boto3
import configparser
import pyarrow
import redshift_connector

URL = {
    "PSI_URL":'https://api.data.gov.sg/v1/environment/pm25', 
    "WEATHER_URL":'https://api.data.gov.sg/v1/environment/2-hour-weather-forecast'
    }

data_source_names = {"PSI_URL":'PSI',
                    "WEATHER_URL": 'WEATHER'
                    }


region_name = 'us-east-1'
bucket_names = {"PSI_URL":'psi-sg',
               "WEATHER_URL":'weather-sg'
               }




config = configparser.RawConfigParser()
aws_credentials_path = '~/.aws/credentials'

# insert the redshift credentials to fit to the testing
redshift_cluster = 'de-project-alan'
redshift_host = 'de-project-alan.cgub1bhr9ywh.us-east-1.redshift.amazonaws.com'
redshift_port = 5439
redshift_database = 'dev'
redshift_user = 'awsuser'
redshift_password = '21J23e91'


# Redshift table
# insert the redshift table to fit to the testing
table = 'psi_table'

fd = open('ddl/table_exists.sql', 'r')
table_exist_query = fd.read().replace('{table}',table)
print(table_exist_query)
fd.close()
print(table_exist_query)

fd1 = open('ddl/table_count_query.sql', 'r')
table_count_query = fd1.read().replace('{table}',table)
print(table_count_query)
fd1.close()

logging.basicConfig(level = logging.INFO)

# Obtain the access key and access key if no hardcoded values were given 
def get_aws_credentials(aws_credentials_path):
    path = os.path.expanduser(aws_credentials_path)
    config_path = config.read(path)
    print(path)
    if os.path.exists(config_path[0]):
        aws_access_key_id = config.get('default', 'aws_access_key_id')
        aws_secret_access_key = config.get('default', 'aws_secret_access_key')
        return aws_access_key_id, aws_secret_access_key
    else:
        logging.error(f'{config_path[0]} is not a aws config file')

def bucket_exist(s3,bucket_name):
    buckets = s3.list_buckets()['Buckets']
    for bucket in buckets:
        if bucket['Name'] == bucket_name:
            logging.info(f'This bucket {bucket_name} is found')

def obtain_key(source_name):
    year = datetime.now().year
    month = datetime.now().month
    day = datetime.now().day
    key = f'year={year}/month={month}/day={day}/{source_name}_{datetime.now()}.parquet'
    return key

def table_exists(cur, table_exist_query):
    cur.execute(table_exist_query)
    if cur.fetchone()[0]:
        logging.info(f"The table '{table}' exists in Redshift.")
    else:
        logging.error(f"The table '{table}' does not exist in Redshift.")

def count_row_table(cursor,query):
    cursor.execute(query)
    return int(cursor.fetchone()[0])

def parameters():
    date_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    return {'date_time':date_time}
def request(url,parameters):
    r = requests.get(url,params=parameters)
    if r.status_code == 200:
        print(f'successful API {r.status_code}')
        return r
    else:
        print(f'error API {r.status_code}')

def get_body_reponse(response):
    if response['api_info']['status'] == 'healthy':
        return response

if __name__ == '__main__':
    # sending get request and saving the response as response object
    aws_access_key_id, aws_secret_access_key = get_aws_credentials(aws_credentials_path)

    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, 
                      aws_secret_access_key=aws_secret_access_key, 
                      region_name=region_name)
    
    conn = redshift_connector.connect(
        host=redshift_host,
        port=redshift_port,
        database=redshift_database,
        user=redshift_user,
        password=redshift_password
    )

    
    final_df = pd.DataFrame()
    for url_link in URL:
        bucket_exist(s3_client,bucket_names[url_link])
        r = request(URL[url_link],parameters())
        content = r.json()
        data = get_body_reponse(content)
        # extracting data in json format
        df = pd.json_normalize(data)
        df_item = pd.json_normalize(df['items'][0])
        df_parquet = df_item.to_parquet(engine='pyarrow')
        s3_key_parquet = obtain_key(data_source_names[url_link])
        print(s3_key_parquet)

        # Load to S3
        s3_client.put_object(
            Bucket=bucket_names[url_link],
            Key=s3_key_parquet,
            Body=df_parquet,  
            ContentType='parquet'  
            )
        
        # cur = conn.cursor()
        # table_exists(cur, table_exist_query)
        # pre_row_count = count_row_table(cur,table_count_query)
        # logging.info(f'{table} table has {pre_row_count} rows')
        