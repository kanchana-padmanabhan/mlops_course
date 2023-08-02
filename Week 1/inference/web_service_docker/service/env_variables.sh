export model_name="monthly_sales"
export model_env="Staging"
export mlflow_tracking_url=http://$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mlflow_dock_con):5000
