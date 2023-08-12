
import os
import sys
import datetime
from dateutil.relativedelta import relativedelta
import math
import pickle

import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.metrics import mean_squared_error
import mlflow
from mlflow.models import infer_signature
from prefect import flow, task
from utils import load_config
from prefect.tasks import task_input_hash
from datetime import timedelta
from config import settings


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def set_registry_parameters(mlflow_url, experiment_name):
    mlflow.set_tracking_uri(mlflow_url)
    mlflow.set_experiment(experiment_name)


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def prepare_data(features_df, train_end_date):
    train_df = features_df[features_df["year_month_first"] <= train_end_date].copy()
    train_df.drop(columns=['year_month_first'], inplace=True)
    train_df.reset_index(drop=True, inplace=True)
    cols = [col for col in train_df.columns if (col != "Monthly_Sales" and col != "Type")]
    X = train_df[cols]
    y = train_df["Monthly_Sales"]
    return X, y


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1), timeout_seconds=300, retries=1, log_prints=True)
def train_model(X, y, tag, prefix="model_"):
    # n_estimators = 10
    # seed= 123
    # tree_method = "approx"
    # enable_categorical = True
    # objective = 'reg:squarederror'
    best_params = {
        "learning_rate": 0.09585355369315604,
        "max_depth": 30,
        "min_child_weight": 1.060597050922164,
        "objective": "reg:squarederror",
        "reg_alpha": 0.018060244040060163,
        "reg_lambda": 0.011658731377413597,
        "seed": 42,
        "max_cat_to_onehot": 1,
       # "early_stopping_rounds": 20
    }
    # mlflow.xgboost.autolog(log_datasets=False) 
    with mlflow.start_run():
        mlflow.set_tag("metadata", tag)
        # mlflow.log_param("n_estimators", n_estimators)
        # mlflow.log_param("seed", 123)
        # mlflow.log_param("tree_method", "approx")
        # mlflow.log_param("enable_categorical", True)
        # mlflow.log_param("objective", 'reg:squarederror')
        mlflow.log_params(best_params)
    
        xgb_r = xgb.XGBRegressor(**best_params) 
            #objective = objective,
                   #   n_estimators = n_estimators, seed = seed, tree_method=tree_method, enable_categorical=enable_categorical, max_cat_to_onehot=1)
        
        xgb_r.fit(X, y)
        signature = infer_signature(X, y)
        # mlflow.xgboost.log_model(xgb_r, input_example=X, artifact_path="model")
        mlflow.xgboost.log_model(xgb_r, signature=signature, artifact_path="model")
        mlflow.end_run()
        return xgb_r
        # ## Save model
        # filename = "models/xgb_retail.bin"
        # os.makedirs(os.path.dirname(filename), exist_ok=True)
        # with open(filename, "wb") as file:
        #     pickle.dump((xgb_r, cols), file)



@flow
def run_training(features_df):
    # input_params = load_config(input_param_file)
    set_registry_parameters(settings.mlflow_url, settings.experiment_name)
    X, y = prepare_data(features_df, settings.train_end_date)
    model = train_model(X, y, tag={"train_end_date": settings.train_end_date}, prefix=settings.prefix)
    return model

# trained_model =  run_training(output_df, input_params)


if __name__ == "__main__":
    features_df = pd.read_csv(settings.sample_features)
    run_training(features_df)