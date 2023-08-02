import os
import mlflow
import pandas as pd
from datetime import datetime, date
import numpy as np
from sqlalchemy import create_engine
import pymysql


name = os.environ["model_name"]#'monthly_sales'
stage = os.environ["model_env"] #'None'

start_date = os.environ["start_date"] #'2010-05-02'
end_date = os.environ["end_date"] #'2010-11-05'
connection_string = os.environ["mysql_connection_string"] # 'mysql+pymysql://application:passpass@127.0.0.1'
database = os.environ["mysql_database"] #'retail_dataset_kaggle'



mlflow.set_tracking_uri(os.environ["mlflow_tracking_url"]) #"http://127.0.0.1:5000"


def load_model(name, stage):
    model = mlflow.pyfunc.load_model(f"models:/{name}/{stage}")
    return model    


def fourier_terms(value, period=30, num_terms=3):
    terms = []
    for i in range(1, num_terms + 1):
        terms.extend([np.sin(2 * np.pi * i * value / period),
                      np.cos(2 * np.pi * i * value / period)])
    return terms



def get_db_connection(mysql_con_string, database_name):
    # sqlEngine       = create_engine('mysql+pymysql://application:passpass@127.0.0.1/retail_data', pool_recycle=3600)
    sqlEngine       = create_engine(mysql_con_string + '/' + database_name, pool_recycle=3600)
    dbConnection    = sqlEngine.connect()
    return dbConnection




def create_inference_context(con, start_date, end_date, frequency='MS'):
    #required_forecasts = sales_df_month[["Store", "Dept"]].drop_duplicates()
    df = pd.read_sql("select DISTINCT Store, Dept from retail_dataset_kaggle.sales", con)
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    date_list = pd.date_range(start_date, end_date, freq=frequency).to_frame()
    context_df = df.merge(date_list, how='cross')
    context_df.columns = ['Store', 'Dept', 'year_month_first']
    context_df['month'] = context_df['year_month_first'].dt.month
    return context_df


def get_features(context_df, start_date, end_date, con):
    store_list = ','.join([str(i) for i in list(context_df['Store'].drop_duplicates())])
    stores_df = pd.read_sql('select Store, Size from retail_dataset_kaggle.store where Store in (' + store_list +')', con)
    predict_df = pd.merge(context_df, stores_df, on="Store")
    
    
    feature_eng_future_df = pd.read_sql('''select *
                                        from retail_dataset_kaggle.store_date_month_agg
                                        where year_month_first >= ''' + start_date + ' AND year_month_first <= ' + end_date, con)

    predict_df = pd.merge(predict_df, feature_eng_future_df, on=["Store", "year_month_first", "month"], how="left")
    predict_df.drop(["Temperature", "Fuel_Price", "CPI", "Unemployment"], axis=1, inplace=True)

    

    pull_forward_df = pd.read_sql('''SELECT t1.Store, t1.Fuel_Price, t1.CPI, t1.Unemployment
                          FROM retail_dataset_kaggle.store_date_month_agg t1
                          JOIN (
                                SELECT Store, MAX(year_month_first) AS max_year_month
                                FROM retail_dataset_kaggle.store_date_month_agg
                                GROUP BY Store
                            ) t2 ON t1.Store = t2.Store AND t1.year_month_first = t2.max_year_month''',con)
    
    temp_same_last_year_df = pd.read_sql('''SELECT t1.Store, t1.month, t1.Temperature
                                            FROM retail_dataset_kaggle.store_date_month_agg t1
                                            JOIN (
                                                SELECT Store, month, MAX(year_month_first) AS max_year_month
                                                FROM retail_dataset_kaggle.store_date_month_agg as t2
                                                GROUP BY Store, month
                                            ) t2 ON t1.Store = t2.Store AND t1.month = t2.month AND t1.year_month_first = t2.max_year_month
                                            ''', con)

    final_features_df = pd.merge(predict_df, pull_forward_df, on=["Store"], how="left")
    final_features_df = pd.merge(final_features_df, temp_same_last_year_df, on=["Store", "month"], how="left")

    final_features_df["monthly_terms"] = final_features_df['month'].apply(fourier_terms)
    final_features_df[['monthly_sin_1', 'monthly_cos_1', 'monthly_sin_2', 'monthly_cos_2', 'monthly_sin_3', 'monthly_cos_3']] = pd.DataFrame(final_features_df['monthly_terms'].to_list())
    final_features_df.drop(columns=["monthly_terms"], inplace=True)
    final_features_df.reset_index(drop=True, inplace=True)
    final_features_df['IsHoliday'] = final_features_df['IsHoliday'].fillna(0)
    return final_features_df
        



def getPythonType(val):
    type = None
    if val == 'DataType.long':
        type = 'Int64'
    if val == 'DataType.double':
        type = 'float'
    return type



def type_check_dataset(input_df, cur_model):
    prediction_df = input_df.copy()
    column_order = [name for name in cur_model.metadata.get_input_schema().input_names()]
    for spec in cur_model.metadata.get_input_schema():
        if spec.name in prediction_df.columns:
            type = getPythonType(str(spec.type))
            if type:
                prediction_df[spec.name] = prediction_df[spec.name].astype(type)
    return prediction_df, column_order


def get_prediction(data, model):
    result = model.predict(data)
    return result



def batch_processing():
    cur_model =  load_model(name, stage)
    con =  get_db_connection(connection_string, database)
    context_df = create_inference_context(con, start_date, end_date)
    prediction_df = get_features(context_df, start_date, end_date, con)
    prediction_df, cols = type_check_dataset(prediction_df, cur_model)



    results = get_prediction(prediction_df[cols], cur_model)



    prediction_df['predicted_monthly_sales'] = results
    prediction_df['model'] = cur_model.metadata.run_id
    prediction_df['prediction_date'] = date.today()
    prediction_df['adjusted_predicted_monthly_sales'] = prediction_df['predicted_monthly_sales'].apply(lambda x: 0 if x < 0 else x)
    prediction_df.to_sql('predicted_monthly_sales_new', con, index=False, if_exists='replace')



    # ### Metrics



    missing_values = len(prediction_df[prediction_df.predicted_monthly_sales.isnull()])
    stats = dict(prediction_df.predicted_monthly_sales.describe())
    number_neg_values = len(prediction_df[prediction_df.predicted_monthly_sales < 0])
    run_metrics_df = pd.DataFrame(columns=['start_date', 'end_date', 'update_date', 'missing_values', 'neg_values', 'count', 'mean', 'min', 'std', '25p', '50p', '75p', 'max'])
    run_metrics_df.loc[0] = [start_date, end_date, date.today(), missing_values, number_neg_values, stats['count'],
                            stats['mean'], stats['std'], stats['min'], stats['25%'], 
                            stats['50%'], stats['75%'], stats['max']]


    run_metrics_df.to_sql('run_metrics', con, index=False, if_exists='append')

    print("Batch Processing Complete; Number of forecasts " + str(len(prediction_df)))


    con.commit()
    con.close()



if __name__ == "__main__":
    batch_processing()
