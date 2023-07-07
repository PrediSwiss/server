from flask import Flask, jsonify, request
from flask_cors import CORS
import gcsfs
import pandas as pd
import pyarrow.parquet as pq
import requests
import json
from geopy.distance import geodesic

# instantiate the app
app = Flask(__name__)
app.config.from_object(__name__)

# enable CORS
CORS(app, resources={r'/*': {'origins': '*'}})



# sanity check route
@app.route('/counter', methods=['GET'])
def get_counter():
    bucket_name = "prediswiss-network"
    file_name = "network.parquet"
    fs_gcs = gcsfs.GCSFileSystem()
    path = bucket_name + "/" + file_name
    table = pq.read_table(path, filesystem=fs_gcs)
    df = table.to_pandas()

    return df.to_json()

@app.route('/test', methods=['GET'])
def testCounter():
    url = "https://us-east1-prediswiss.cloudfunctions.net/predict"

    headers = {
        'Content-Type': 'application/json'
    }

    payload = {
        "id": 'CH:0119.01',
        "time": 3600,
        "store": False
    }

    response = requests.post(url, json=payload, headers=headers)

    return response.text

@app.route('/trip', methods=['POST'])
def get_trip():
    bucket_name = "prediswiss-network"
    file_name = "network.parquet"
    fs_gcs = gcsfs.GCSFileSystem()
    path = bucket_name + "/" + file_name
    table = pq.read_table(path, filesystem=fs_gcs)
    df = table.to_pandas()

    df['lat'] = df['lat'].astype(float)
    df['long'] = df['long'].astype(float)

    data = request.json

    max_lat = min_lat = data[0]['lat']
    max_lng = min_lng = data[0]['lng']

    # Find the maximum and minimum values for latitude and longitude
    for item in data:
        max_lat = max(max_lat, item['lat'])
        min_lat = min(min_lat, item['lat'])
        max_lng = max(max_lng, item['lng'])
        min_lng = min(min_lng, item['lng'])

    # Apply the threshold of 5 meters
    threshold = 0.00045  # Approximately 5 meters in latitude or longitude
    max_lat += threshold
    min_lat -= threshold
    max_lng += threshold
    min_lng -= threshold

    # Restrict the dataframe based on the bounds
    restricted_df = df[
        (df['lat'].between(min_lat, max_lat)) &
        (df['long'].between(min_lng, max_lng))
    ]

    # Assign in_path values
    df['in_path'] = restricted_df.apply(lambda row: any(geodesic((row['lat'], row['long']), (coord['lat'], coord['lng'])).m <= 40 for coord in data), axis=1)

    # Filter the DataFrame to include only the rows in the path
    filtered_df = df.fillna({'in_path': False})
    filtered_df = filtered_df[filtered_df['in_path'] == True]

    url = "https://us-east1-prediswiss.cloudfunctions.net/predict"
    headers = {
        'Content-Type': 'application/json'
    }

    for index, row in df.iterrows():
        payload = {
            "id": row['id'],
            "time": 3600,
            "store": False
        }

        response = requests.post(url, data=json.dumps(payload), headers=headers)
        print(response.status_code)
        print(response)

        break
    
    return "ok"

if __name__ == '__main__':
    app.run()