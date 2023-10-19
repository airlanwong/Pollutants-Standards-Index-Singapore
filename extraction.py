import requests
import json
import pandas as pd
from datetime import datetime
import os
import logging
import boto3
import configparser
import pyarrow

URL = 'https://api.data.gov.sg/v1/environment/pm25'
region_name = 'us-east-1'
bucket_name = 'psi-sg'
data_source_name = 'PSI'

config = configparser.RawConfigParser()
aws_credentials_path = '~/.aws/credentials'

# insert the redshift credentials to fit to the testing
redshift_cluster = ''
redshift_host = ''
redshift_port = 5439
redshift_database = ''
redshift_user = ''
redshift_password = ''
iam = ''


# Redshift table
# insert the redshift table to fit to the testing
table = ''

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
    bucket_exist(s3_client,bucket_name)
    r = request(URL,parameters())
    content = r.json()
    data = get_body_reponse(content)
    # extracting data in json format
    df = pd.json_normalize(data)
    print(df)
    df_parquet = df.to_parquet(engine='pyarrow')
    s3_key_parquet = obtain_key(data_source_name)


    # Load to S3
    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key_parquet,
        Body=df_parquet,  
        ContentType='parquet'  
        )