# Use the official Python base image
# FROM python:3.9-slim
FROM python:3.8.17

# Set the working directory in the container
WORKDIR /app

# Copy the Flask app file into the container

COPY mlflow_requirements.txt .
# COPY notebooks/WebService_Endpoint.py .

# Install the required dependencies
RUN python -m pip install --no-cache-dir --disable-pip-version-check -r mlflow_requirements.txt


# RUN mkdir -p mlartifacts
# COPY ../mlartifacts/* /app/mlartifacts

# Expose the port on which the Gunicorn server will run
EXPOSE 5000

# Run the Gunicorn server with 4 workers (you can adjust the number of workers as needed)
CMD ["mlflow", "ui", "--host", "0.0.0.0"]