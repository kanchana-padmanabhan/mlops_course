import os
import sys
import datetime
from dateutil.relativedelta import relativedelta
import math
import pickle

import pandas as pd
import numpy as np
import pymysql
from flytekit import Resources, task, workflow, kwtypes
from utils import fourier_terms, load_config
from datetime import timedelta
import time
from config import settings
from flytekitplugins.sqlalchemy import SQLAlchemyConfig, SQLAlchemyTask


DATABASE_URI = (settings.connection_string + '/' + settings.database)


extract_data = SQLAlchemyTask(
    "query_data",
    query_template=(
        "select * from {{.inputs.table}}"
    ),
    inputs=kwtypes(table=str),
    output_schema_type=pd.DataFrame,
    task_config=SQLAlchemyConfig(uri=DATABASE_URI),
)

@task
def merge_features(features_df: pd.DataFrame, sales_df: pd.DataFrame, stores_df: pd.DataFrame) -> pd.DataFrame:
    feature_eng_df = pd.merge(stores_df, sales_df, on="Store")
    feature_eng_df = pd.merge(features_df, feature_eng_df, on=["Store", "year_month_first"])
    feature_eng_df['year_month_first'] =  pd.to_datetime(feature_eng_df['year_month_first'])
    feature_eng_df["month"] = feature_eng_df['year_month_first'].dt.month
    return feature_eng_df


@task
def add_time_features(feature_eng:pd.DataFrame, period:int, num_terms:int, prefix:str) -> pd.DataFrame:
    term_names_sin =  [prefix + '_sin_' + str(i) for i in range(1, num_terms+1)]
    term_names_cos =  [prefix + '_cos_' + str(i) for i in range(1, num_terms+1)]
    term_names = [None]*(len(term_names_sin)+len(term_names_cos))
    term_names[::2] = term_names_sin
    term_names[1::2] = term_names_cos
    feature_eng['terms'] = feature_eng['month'].apply(fourier_terms, args=(period, num_terms))
    feature_eng[term_names] = pd.DataFrame(list(feature_eng['terms']))
    feature_eng.drop(columns=["terms"], inplace=True)
    feature_eng.reset_index(drop=True, inplace=True)
    return feature_eng    


@workflow
def run_feature_engineering() -> pd.DataFrame:
    features_df = extract_data(table="store_date_month_agg")
    sales_df = extract_data(table="sales_monthly_agg")
    store_df = extract_data(table="store")
    feature_eng_df = merge_features(features_df=features_df, sales_df=sales_df, stores_df=store_df)
    feature_final_df = add_time_features(feature_eng=feature_eng_df, period=30, num_terms=3, prefix="monthly")
    return feature_final_df


