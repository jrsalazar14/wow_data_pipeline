import requests
import json
import os
import dotenv
from google.cloud import storage

dotenv.load_dotenv()

# WoW API Credentials
client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')

def get_access_token(client_id, client_secret):
    url = "https://us.battle.net/oauth/token"
    data = { 
        "grant_type": "client_credentials",
    }
    response = requests.post(url, data, auth=(client_id, client_secret))
    token_info = response.json()
    return token_info['access_token']

def get_races(access_token):
    url = "https://us.api.blizzard.com/data/wow/playable-race/index"
    params = {
        "access_token": access_token,
        "namespace": "static-us",
        "locale": "en_US"
    }
    response = requests.get(url, params=params)
    return response.json()

def upload_to_gcs(bucket_name, data, filename):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    if isinstance(filename, dict):
        filename = "races.json"
    blob = bucket.blob(filename)
    blob.upload_from_string(json.dumps(data), content_type='application/json')
    print(f'File {filename} uploaded to {bucket_name} bucket')

if __name__ == '__main__':
    access_token = get_access_token(client_id, client_secret)
    races_data = get_races(access_token)
    print(races_data)

    upload_to_gcs('airflow_wow_data_bucket', 'wow_data/races.json', races_data)
