import os
import pickle
from datetime import datetime, date

import numpy as np
import pandas as pd

from flask import Flask, request, jsonify
import xgboost
import pickle
from minio import Minio
import json
import hashlib
import time
import io


app = Flask(__name__)


# name = 'monthly_sales'
# stage = 'Staging'
period = 30
num_terms = 3

minio_endpoint = os.environ.get("MINIO_ENDPOINT")
minio_access_key = os.environ.get("MINIO_ACCESS_KEY")
minio_secret_key = os.environ.get("MINIO_SECRET_KEY")
minio_client = Minio(
    minio_endpoint,
    access_key=minio_access_key,
    secret_key=minio_secret_key,
    secure=False
)

try:
    temp  = pickle.load(open(os.environ["model_file"], 'rb'))
    model = temp[0]
    model_cols = temp[1]
except:
    print("Could not load model")


def fourier_terms(value, period, num_terms):
    terms = []
    for i in range(1, num_terms + 1):
        terms.extend([np.sin(2 * np.pi * i * value / period),
                      np.cos(2 * np.pi * i * value / period)])
    term_names_sin =  ['monthly_sin_' + str(i) for i in range(1, num_terms+1)]
    term_names_cos =  ['monthly_cos_' + str(i) for i in range(1, num_terms+1)]
    term_names = [None]*(len(term_names_sin)+len(term_names_cos))
    term_names[::2] = term_names_sin
    term_names[1::2] = term_names_cos
    return dict(zip(term_names, terms))


def prepare_features(input):
    if not model:
        return None
    input['month'] = np.int32(datetime.strptime(input['year_month'], '%Y-%m').month) 
    #do it cleanly. Read types from model input schema
    ft = fourier_terms(input['month'], 30, 3)
    input.update(ft.items())
    input_final = {name:input[name] for name in model_cols}
    return input_final


def predict(features):
    if not model:
        return None
    df =  pd.DataFrame([features.values()],columns=features.keys())
    preds = model.predict(df)
    temp = {}
    for key, value in features.items():
        if type(value) == np.int32:
            temp[key] = int(value)
        elif type(value) == np.float32:
            temp[key] = float(value)
        else:
            temp[key] = value
    data_to_upload = json.dumps({"result": float(preds[0])})
    data_stream = io.BytesIO(data_to_upload.encode())
    hash = hashlib.sha1()
    hash.update(str(time.time()).encode("utf-8"))
    id_val = hash.hexdigest()
    minio_client.put_object(
    os.environ["MINIO_BUCKET"],
    "pred_" + id_val[0:5] + ".txt",
    data_stream,
    len(data_to_upload)
)
    return float(preds[0])


@app.route('/', methods=['POST'])
def home():
    return "Monthly_Sales Flask App"


@app.route('/predict', methods=['POST'])
def predict_endpoint():
    input_fea = request.get_json()

    features = prepare_features(input_fea)
    if features:
        pred = predict(features)
    else:
        pred = None

    status = "OK"

    if not pred:
        status = "error"
        pred = np.nan



    result = {
        'input_features': pd.DataFrame.from_dict([input_fea]).to_json(),
        'monthly_sales': pred,
        'model_version': str(model),
        'metadata': {'period': 30, 'num_terms': 3},
        'date_of_prediction': date.today()
    }

    return jsonify(result)


if __name__ == "__main__":
    app.run()