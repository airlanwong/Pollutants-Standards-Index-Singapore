import requests
import json
import pandas as pd
from datetime import datetime


URL = 'https://api.data.gov.sg/v1/environment/pm25'

def PARAMS():
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
    r = request(URL,PARAMS())
    content = r.json()
    data = get_body_reponse(content)
    # extracting data in json format
    df = pd.json_normalize(data)
    print(df)
    df.to_csv(r'C:\Users\User\Downloads\data.csv')