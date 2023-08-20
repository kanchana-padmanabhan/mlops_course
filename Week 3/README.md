# Setting up Kubenetes
Minikube
Minikube is local Kubernetes, focusing on making it easy to learn and develop for Kubernetes.

Helm - package manager for K8s (like pip)

### Getting setup with Minikube with following commands
https://minikube.sigs.k8s.io/docs/start/

```brew install minikube```
```brew install helm```

```minikube start```

```alias kubectl="minikube kubectl --"```



## Set a common namespace to be used on K8s for all services
```kubectl config set-context --current --namespace=argo```

## Setting up Minio - s3-like object storage (acts as artifact storage) on K8s
* Set up service on K8s
```kubectl apply -f minio-dev-loadbalancer.yaml ```
* Tunnel the UI to a local port
```minikube service minio-service -n argo```
* Go to the UI and create an Access Key
  -- http://127.0.0.1:61711/access-keys (port number should be taken from previous command)
* Also on UI create a bucket called **argo-bucket** (you can provide any name you want)
  -- http://127.0.0.1:61711/buckets
* Copy and set up the access key on K8s as part of secrets
```
kubectl create secret generic argo-artifacts \
    --from-literal=accesskey=5NjRLPAm2R5xeIoj \
    --from-literal=secretkey=MEQUeLpKteJ3Ids1mD4mQZLtvzgCVHBq \
    -n argo
```
* Set up a Config map to make bucket accessible to all services
```kubectl apply -f artifact-repositories.yaml```


## Argo - workflow tool that works nicely on kubenetes
Example: https://blog.devgenius.io/unleash-your-pipeline-creativity-local-development-with-argo-workflows-and-minio-on-minikube-7537642b7b1d (use the commands I provided)

* Deploy argos server on K8s
```kubectl apply -n argo -f https://github.com/argoproj/argo-workflows/releases/latest/download/argo-server.yaml```


* Authentication mode for Argo Workflows to “server” for enhanced security.
```
kubectl patch deployment \ 
  argo-server \
  --namespace argo \
  --type='json' \
  -p='[{"op": "replace", "path": "/spec/template/spec/containers/0/args", "value": [
  "server",
  "--auth-mode=server"
]}]'
```

* Create a role called default-admin
```
kubectl create rolebinding default-admin --clusterrole=admin --serviceaccount=argo:default --namespace=argo
```
* Port forward to local port to access the argo ui
```kubectl port-forward -n argo svc/argo-server 2746:2746```

* Go to UI and click on submit new workflow 
 -- Go to https://localhost:2746/workflows
 -- Copy this contents or upload the workflow: test_workflow.yml 
 -- Execute via UI
 -- You should be able to see the output written to argo-bucket (see it on MINIO UI)
 -- if you want to redo any of the steps, don't forget to restart the K8s pods for argo-server
``kubectl rollout restart deployment/argo-server -n argo``
* Another way to do to do port forwarding is via ingress (port forwarding is a nice way to test locally)
https://kubernetes.io/docs/concepts/services-networking/ingress/
https://matthewpalmer.net/kubernetes-app-developer/articles/kubernetes-ingress-guide-nginx-example.html
```helm install ingress-nginx ingress-nginx/ingress-nginx```
```kubectl apply -f ingress-argo.yaml```



## Testing your web inference workflow on k8s
* Run and scale that web inference pipeline on kubenetes
* Erite some outputs to an object storage (minio) - like s3 but allows local testing



### Build and store the image on k8s (run this command in the web_inference/service folder)
* Build image
```minikube image build -t webservice_inference_k8:latest .```
* Check if image is available on minikube
```minikube image ls```
* Note that I copied the model file with the image this time
### Apply the yml file to set up the service and loadbalancer for web inference
```kubectl apply -f web_service.yml -n argo```

### Commands to ensure the service is up and running
* Check the pods are running (you should see a pod with prefix "predict-service-deployment)
```kubectl get pods```

* To make sure the right image was applied to the service try
```kubectl describe pod pod_name``

* To login to a pod the verify any logs
```kubectl exec  pod_name  -it -- /bin/sh```

* Check if loadbalancer is running ok (you should see predict-web-service)
```kubectl get svc```

* Tunnel the web service endpoint
```minikube service predict-web-service -n argo```
(This gives you the local portnumber to test your service)
* Use the http://127.0.0.1:portnumber to test the POST service
You can use something like POSTMAN or curl as a test the endpoint
```
curl -X POST -H "Content-Type: application/json" -d '{
    "year_month": "2023-09",
    "Store": 4,
    "Temperature": 52.285,
    "Fuel_Price": 4.4595,
    "MarkDown1": 16931.265,
    "MarkDown2": 7281.18,
    "MarkDown3": 68.7925,
    "MarkDown4": 12626.244999999999,
    "MarkDown5": 601.6725,
    "CPI": 220.37496355,
    "Unemployment": 7.348,
    "IsHoliday": 1,
    "Size": 151315,
    "Dept": 7
}' http://127.0.0.1:60835/predict
```
* To ensure the web service is avaiable on k8s also check
```minikube service list```

* The yml file only deploys one pod for this service. If you want to scale it you can do the following
```kubectl scale deployment predict-service-deployment --replicas=2```
    You will see 2 pods now with the prefix "predict-service-deployment"
    You can also scale down the same way
* Additional notes to manage images on minikube
-- Delete old unused images. They take up space and you will run out during local testing
-- Get a list of all images
    ```minikube image ls --format json ```
-- Find the IDs of images that are dangling (typically you will see <none>:<none> at the end of the name) and delete them
```minikube image rm image_id```



# Set up and Run Spark

## Install python dependencies
```pip install -r requirements.txt```

## Download SPARK binary and unzip it
https://spark.apache.org/downloads.html

## Set up environment variables
export SPARK_HOME="/Users/kanchanapadmanabhan/OneDrive/Personal-Course/Vector/Model Deployment/mlops_course/Week 3/spark/spark-3.4.1-bin-hadoop3"
export PATH=$SPARK_HOME/bin:$PATH

## Test the jupyter notebooks under **spark** folder



# Monitoring data and models
###  Install requirements
```pip install -r requirements.txt```
### Sample notebooks unde model data monitoring for Reports and Tests
### Sample Moniting
```evidently ui --demo-project```
### Monitoring results from notebok
* Run ```evidently ui ```
* Click on "New Project"

## References
* https://towardsdatascience.com/a-beginner-friendly-introduction-to-kubernetes-540b5d63b3d7
* https://medium.com/avmconsulting-blog/running-a-python-application-on-kubernetes-aws-56609e7cd88c
* https://blog.devgenius.io/unleash-your-pipeline-creativity-local-development-with-argo-workflows-and-minio-on-minikube-7537642b7b1d
* https://cloud.google.com/blog/products/databases/to-run-or-not-to-run-a-database-on-kubernetes-what-to-consider
* https://levelup.gitconnected.com/two-easy-ways-to-use-local-docker-images-in-minikube-cd4dcb1a5379
* https://testdriven.io/blog/running-flask-on-kubernetes/
* https://github.com/kubernetes/minikube/issues/16176
* https://stackoverflow.com/questions/65397050/minikube-does-not-start-on-ubuntu-20-04-lts-exiting-due-to-guest-provision
*https://blog.devgenius.io/unleash-your-pipeline-creativity-local-development-with-argo-workflows-and-minio-on-minikube-7537642b7b1d
* https://medium.com/@mehmetodabashi/installing-argocd-on-minikube-and-deploying-a-test-application-caa68ec55fbf
* https://medium.com/mlearning-ai/easy-analysis-of-your-data-and-ml-model-using-evidently-ai-830ef0c1c4fd

