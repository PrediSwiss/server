from flask import Flask, jsonify, request
from flask_cors import CORS
import gcsfs
import pandas as pd
import pyarrow.parquet as pq
import requests
from geopy.distance import geodesic
import asyncio
import aiohttp
import json
import xml.etree.ElementTree as ET


# flask run --port=5001 --debug

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

@app.route('/current', methods=['GET'])
def get_current():
    bucket_name = "prediswiss-raw-data"
    fs_gcs = gcsfs.GCSFileSystem()
    lastFolder = fs_gcs.ls(bucket_name)[-1]
    lastFile = fs_gcs.ls(lastFolder)[-1]

    content = None
    with fs_gcs.open(lastFile, 'r') as file:
        content = file.read()

    namespaces = {
        'ns0': 'http://schemas.xmlsoap.org/soap/envelope/',
        'ns1': 'http://datex2.eu/schema/2/2_0',
    }

    dom = ET.fromstring(content)

    publicationDate = dom.find(
        './ns0:Body'
        '/ns1:d2LogicalModel'
        '/ns1:payloadPublication'
        '/ns1:publicationTime',
        namespaces
    ).text

    compteurs = dom.findall(
        './ns0:Body'
        '/ns1:d2LogicalModel'
        '/ns1:payloadPublication'
        '/ns1:siteMeasurements',
        namespaces
    )

    flowTmp = [(compteur.find(
        './ns1:measuredValue[@index="1"]'
        '/ns1:measuredValue'
        '/ns1:basicData'
        '/ns1:vehicleFlow'
        '/ns1:vehicleFlowRate',
        namespaces
    ), compteur.find(
        './ns1:measuredValue[@index="11"]'
        '/ns1:measuredValue'
        '/ns1:basicData'
        '/ns1:vehicleFlow'
        '/ns1:vehicleFlowRate',
        namespaces
    ), compteur.find(
        './ns1:measuredValue[@index="21"]'
        '/ns1:measuredValue'
        '/ns1:basicData'
        '/ns1:vehicleFlow'
        '/ns1:vehicleFlowRate',
        namespaces
    )) for compteur in compteurs]

    speedTmp = [(compteur.find(
        './ns1:measuredValue[@index="2"]'
        '/ns1:measuredValue'
        '/ns1:basicData'
        '/ns1:averageVehicleSpeed'
        '/ns1:speed',
        namespaces
    ), compteur.find(
        './ns1:measuredValue[@index="12"]'
        '/ns1:measuredValue'
        '/ns1:basicData'
        '/ns1:averageVehicleSpeed'
        '/ns1:speed',
        namespaces
    ), compteur.find(
        './ns1:measuredValue[@index="22"]'
        '/ns1:measuredValue'
        '/ns1:basicData'
        '/ns1:averageVehicleSpeed'
        '/ns1:speed',
        namespaces
    )) for compteur in compteurs]

    speed = []
    for sp in speedTmp:
        speed.append((None if sp[0] == None or sp[0] == 0 else sp[0].text, None if sp[1] == None or sp[1] == 0 else sp[1].text, None if sp[2] == None or sp[2] == 0 else sp[2].text))

    flow = []
    for fl in flowTmp:
        flow.append((None if fl[0] == None else fl[0].text, None if fl[1] == None else fl[1].text, None if fl[2] == None else fl[2].text))

    compteursId = [comp.find('ns1:measurementSiteReference', namespaces).get("id") for comp in compteurs]

    columns = ["publication_date", "id", "flow_1", "flow_11", "flow_21", "speed_2", "speed_12", "speed_22"]

    data = [(publicationDate, compteursId[i], flow[i][0], flow[i][1], flow[i][2], speed[i][0], speed[i][1], speed[i][2]) for i in range(len(compteursId)) ]
    dataframe = pd.DataFrame(data=data, columns=columns)

    return dataframe.to_json()

@app.route('/tripPredict', methods=['POST'])
async def get_trip_predict():
    data = request.json

    url = "https://us-east1-prediswiss.cloudfunctions.net/predict"

    headers = {
        'Content-Type': 'application/json'
    }

    responses = []
    async with aiohttp.ClientSession() as session:
        for value in data[0].values():
            payload = {
                "id": value,
                "date": data[1],
                "hour": data[2],
                "store": False
            }
            response = asyncio.ensure_future(make_request(session, url, payload, headers))
            responses.append(response)

        await asyncio.gather(*responses)

    responsesData = [pd.DataFrame(json.loads(response.result())) if response.result() != "" else None for response in responses]

    time = None
    predict = []

    for dataframe in responsesData:
        if dataframe is not None:
            if 'ds' in dataframe.columns and len(dataframe['ds']) > 0:
                print(dataframe['ds'].iloc[-1])
                print(int(dataframe['ds'].iloc[-1]))
                time = int(dataframe['ds'].iloc[-1])
            if 'yhat' in dataframe.columns and len(dataframe['yhat']) > 0:
                predict.append(dataframe['yhat'].iloc[-1])
            else:
                predict.append(-1)

    result = {
        "predict": predict,
        "time": time
    }

    return result

async def make_request(session, url, payload, headers):
    async with session.post(url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=540)) as response:
        response = await response.text()
        print("Request finish")
        return response

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

    max_lat = min_lat = data[0][0]['lat']
    max_lng = min_lng = data[0][0]['lng']

    # Find the maximum and minimum values for latitude and longitude
    for item in data[0]:
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
    df['in_path'] = restricted_df.apply(lambda row: any(geodesic((row['lat'], row['long']), (coord['lat'], coord['lng'])).m <= 40 for coord in data[0]), axis=1)

    # Filter the DataFrame to include only the rows in the path
    filtered_df = df.fillna({'in_path': False})
    filtered_df = filtered_df[filtered_df['in_path'] == True]

    return filtered_df.to_json()    

if __name__ == '__main__':
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(asyncio.ensure_future(get_trip_predict()))
    app.run()