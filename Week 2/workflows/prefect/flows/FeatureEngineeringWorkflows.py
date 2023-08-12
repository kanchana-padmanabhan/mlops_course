import os
import sys
import datetime
from dateutil.relativedelta import relativedelta
import math
import pickle

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import pymysql
from prefect import flow, task, get_run_logger
from utils import fourier_terms, load_config
from prefect.tasks import task_input_hash
from datetime import timedelta
import time
from config import settings



@task
def get_db_connection(mysql_con_string, database_name):
    # sqlEngine       = create_engine('mysql+pymysql://application:passpass@127.0.0.1/retail_data', pool_recycle=3600)
    sqlEngine       = create_engine(mysql_con_string + '/' + database_name, pool_recycle=3600)
    dbConnection    = sqlEngine.connect()
    return dbConnection

@task 
def close_db_connection(con):
    con.close()

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1), retries=1)
def calculate_store_date_features(db_connection):
    features_df  = pd.read_sql("select * from retail_dataset_kaggle.store_date_month_agg", db_connection)
    return features_df


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1), retries=1)
def calculate_sales_features(db_connection):
    sales_df =  pd.read_sql("select * from retail_dataset_kaggle.sales_monthly_agg", db_connection)
    return sales_df


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1), retries=1)
def calculate_store_features(db_connection):
    stores_df = pd.read_sql("select * from retail_dataset_kaggle.store", db_connection)
    return stores_df


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1), log_prints=True, retries=1)
def merge_features(features_df, sales_df, stores_df):
    feature_eng_df = pd.merge(stores_df, sales_df, on="Store")
    feature_eng_df = pd.merge(features_df, feature_eng_df, on=["Store", "year_month_first"])
    feature_eng_df['year_month_first'] =  pd.to_datetime(feature_eng_df['year_month_first'])
    feature_eng_df["month"] = feature_eng_df['year_month_first'].dt.month
    return feature_eng_df


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1), retries=1)
def add_time_features(feature_eng, period=30, num_terms=3, prefix="monthly"):

    term_names_sin =  [prefix + '_sin_' + str(i) for i in range(1, num_terms+1)]
    term_names_cos =  [prefix + '_cos_' + str(i) for i in range(1, num_terms+1)]
    term_names = [None]*(len(term_names_sin)+len(term_names_cos))
    term_names[::2] = term_names_sin
    term_names[1::2] = term_names_cos
    feature_eng['terms'] = feature_eng['month'].apply(fourier_terms, args=(period, num_terms))
    feature_eng[term_names] = pd.DataFrame(feature_eng['terms'].to_list())
    feature_eng.drop(columns=["terms"], inplace=True)
    feature_eng.reset_index(drop=True, inplace=True)
    return feature_eng    


@flow
def run_feature_engineering():
    logger = get_run_logger()
    # input_params = load_config(input_param_file)
    con = get_db_connection(mysql_con_string=settings.connection_string, database_name=settings.database)
    features_df = calculate_store_date_features(db_connection=con)
    logger.info("Features_df count " + str(len(features_df)))
    sales_df = calculate_sales_features(db_connection=con)
    logger.info("sales_df count " + str(len(sales_df)))
    store_df = calculate_store_features(db_connection=con)
    logger.info("Store_df count " + str(len(store_df)))
    feature_eng_df = merge_features(features_df=features_df, sales_df=sales_df, stores_df=store_df)
    feature_final_df = add_time_features(feature_eng_df, num_terms=3, prefix="monthly")
    logger.info("feature_final_df count " + str(len(feature_final_df)))
    logger.info("feature_final_df schema " + str(feature_final_df.dtypes))
    close_db_connection(con)
    return feature_final_df



if __name__ == "__main__":
    run_feature_engineering()
# output_df = run_feature_engineering()
