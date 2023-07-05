from flask import Flask, jsonify
from flask_cors import CORS
import gcsfs
import pandas as pd
import pyarrow.parquet as pq

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


if __name__ == '__main__':
    app.run()