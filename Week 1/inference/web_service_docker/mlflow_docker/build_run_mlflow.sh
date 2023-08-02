docker network create -d bridge mlops_bridge
docker build -t mlflow_docker . 
docker run -it --network=mlops_bridge -p 80:5000  --name mlflow_dock_con mlflow_docker
