gunicorn -w 4 --bind 0.0.0.0:9696 --access-logfile monthly_sales_gunicorn_access.log --error-logfile monthly_sales_gunicorn_error.log  --log-level INFO --statsd-host=localhost:8125 --statsd-prefix=monthly_sales.app WebService_Endpoint:app