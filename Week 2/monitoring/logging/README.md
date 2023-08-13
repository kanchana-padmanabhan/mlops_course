Let's turn on Loki logging so we can also capture all the system logs and analyze them using Grafana. 
1. We will use the  docker-compose.yml file in this folder to set it up. The following command to pull images and run the container. 
```docker-compose  -f docker-compose.yml up```
2. We will keep the same docker network "statsd" so that all servcies can be accessible from Grafana
3. We will use the Grafana we already set up in the "inference_monitor" folder  (That has been commented out from this docker-compose.yml)
4. Key thing to update in the docker-compose.yml is the location of the log-files for Promtal - **volumes:** setting. Promtail is an agent that ships the contents of local logs to local/remote Loki. 