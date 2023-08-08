echo $model_name
echo $model_env

gunicorn -w 4 --bind 0.0.0.0:9696 --access-logfile monthly_sales_gunicorn_access.log --error-logfile monthly_sales_gunicorn_error.log  --log-level INFO WebService_Endpoint:app