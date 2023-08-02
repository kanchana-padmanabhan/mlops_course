import os
import pickle
from datetime import datetime, date

import numpy as np
import pandas as pd

import mlflow
from flask import Flask, request, jsonify
import xgboost



app = Flask(__name__)


name = os.environ['model_name'] #'monthly_sales'
stage = os.environ['model_env'] #'Staging'
# name = 'monthly_sales'
# stage = 'Staging'
period = 30
num_terms = 3

mlflow.set_tracking_uri(os.environ['mlflow_tracking_url'])
try:
   # print("Model Loaded from " + os.environ['mlflow_tracking_url'])
    model = mlflow.pyfunc.load_model(f"models:/{name}/{stage}")
    # RUN_ID = os.environ['RUN_ID']
    #  logged_model = f'mlflow_models/{RUN_ID}/artifacts/model'
    #model = mlflow.pyfunc.load_model(logged_model)
    model_version = model.metadata.run_id
except:
    model = None
    model_version = "Error"
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
    input_final = {name:input[name] for name in model.metadata.get_input_schema().input_names()}
    return input_final


def predict(features):
    if not model:
        return None
    preds = model.predict(features)
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
        'model_version': model_version,
        'metadata': {'period': 30, 'num_terms': 3},
        'date_of_prediction': date.today()
    }

    return jsonify(result)


if __name__ == "__main__":
    app.run()