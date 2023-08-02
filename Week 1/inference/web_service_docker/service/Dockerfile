# Use the official Python base image
#FROM python:3.9-slim
FROM python:3.8.17

RUN adduser --disabled-password appuser
WORKDIR /home/appuser/


COPY --chown=appuser:appuser web_service_requirements.txt .
COPY --chown=appuser:appuser WebService_Endpoint.py .

# COPY mlartifacts/802893463743823447/* mlflow_models/

# Set the working directory in the container
# WORKDIR /app

# Copy the Flask app file into the container

# COPY web_service_requirements.txt .
# COPY notebooks/WebService_Endpoint.py .

# Install the required dependencies
RUN python -m pip install -r web_service_requirements.txt
# --no-cache-dir --disable-pip-version-check -r web_service_requirements.txt


USER appuser

# Expose the port on which the Gunicorn server will run
EXPOSE 9696

# Run the Gunicorn server with 4 workers (you can adjust the number of workers as needed)
CMD ["gunicorn", "-b", "0.0.0.0:9696", "-w", "4", "--timeout", "600", "--log-level", "debug", "--access-logfile", "monthly_sales_gunicorn_access.log", "--error-logfile", "monthly_sales_gunicorn_error.log", "WebService_Endpoint:app"]

# "--env", "model_name=$MODEL_NAME", "--env", "model_env=$MODEL_ENV", "--env", "mlflow_tracking_url=$MLFLOW_URL", "WebService_Endpoint"]
