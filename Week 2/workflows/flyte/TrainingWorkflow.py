
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
from flytekit import Resources, task, workflow
from datetime import timedelta
from config import settings
from flytekitplugins.mlflow import mlflow_autolog


# @task
# def set_registry_parameters(mlflow_url: str, experiment_name: str):
#     mlflow.set_tracking_uri(mlflow_url)
#     mlflow.set_experiment(experiment_name)

mlflow.set_tracking_uri(settings.mlflow_url)

@task
def prepare_data(features_df: pd.DataFrame, train_end_date: str) -> (pd.DataFrame, pd.Series):
    train_df = features_df[features_df["year_month_first"] <= train_end_date].copy()
    train_df.drop(columns=['year_month_first'], inplace=True)
    train_df.reset_index(drop=True, inplace=True)
    cols = [col for col in train_df.columns if (col != "Monthly_Sales" and col != "Type")]
    X = train_df[cols]
    y = train_df["Monthly_Sales"]
    return X, y


@task(retries=1)
@mlflow_autolog(framework=mlflow.xgboost, experiment_name=settings.experiment_name)
def train_model(X: pd.DataFrame, y: pd.Series, tag: dict, prefix: str):
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
    mlflow.xgboost.autolog(log_datasets=False) 
    #with mlflow.start_run() as run:
    mlflow.set_tag("metadata", tag)
    mlflow.log_params(best_params)

    xgb_r = xgb.XGBRegressor(**best_params) 

    xgb_r.fit(X, y)
    signature = infer_signature(X, y)
    mlflow.xgboost.log_model(xgb_r, signature=signature, artifact_path="model")


@workflow
def run_training(features_df: pd.DataFrame):
    # input_params = load_config(input_param_file)
    #set_registry_parameters(mlflow_url=settings.mlflow_url, experiment_name=settings.experiment_name)
    X, y = prepare_data(features_df=features_df, train_end_date=settings.train_end_date)
    train_model(X=X, y=y, tag={"train_end_date": settings.train_end_date}, prefix=settings.prefix)
