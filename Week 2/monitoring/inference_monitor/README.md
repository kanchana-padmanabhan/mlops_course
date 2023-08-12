## Commands
```txt
+--------------------+                     +-------------------+                        +--------------+               +-----------+
|  Gunicorn(StatsD)  |---(UDP repeater)--->|  statsd_exporter  |<---(scrape /metrics)---|  Prometheus  | <---(query)---|  Grafana  |
+--------------------+                     +-------------------+                        +--------------+               +-----------+
```


### Create a Docker Network for all services

```docker network create -d bridge statsd   ```

### StatsD Exporter (https://hub.docker.com/r/prom/statsd-exporter)

```docker pull prom/statsd-exporter```

```
docker run -d \
  --network=statsd \
  -p 9102:9102 \
  -p 9125:9125 \
  -p 9125:9125/udp \
  -v ${PWD}/statsd_mapping.yml:/tmp/statsd_mapping.yml \
  prom/statsd-exporter --statsd.mapping-config=/tmp/statsd_mapping.yml
  ```



### Prometheus (https://hub.docker.com/r/prom/prometheus)

Extract IP of statsd container and update prometheus.yml

```docker run -d \
    --network=statsd \
    -p 9090:9090 \
    -v ${PWD}/prometheus.yml:/etc/prometheus/prometheus.yml \
    prom/prometheus```


### Grafana

Extract IP of prometheus container

```docker run --network=statsd -d --name=grafana -p 3000:3000 grafana/grafana-enterprise```

Configure grafana -> prometheus connection on the UI using the IP addres of the prometheus container

### Gunicorn
Start Gunicorn server with command in ```gunicorn_stats_prom.sh``` (in the folder from Week 1/inference/web_inference)

Run following command to generate lots of requests


```for i in $(seq 500); do
    curl --location 'localhost:9696/predict' \
    --header 'Content-Type: application/json' \
    --data '{
        "year_month": "2023-09",
        "Store": 4,
        "Temperature": 52.285,
        "Fuel_Price": 4.4595,
        "MarkDown1": 16931.265,
        "MarkDown2": 7281.18,
        "MarkDown3": 68.7925,
        "MarkDown4": 12626.245,
        "MarkDown5": 601.6725,
        "CPI": 220.37496355,
        "Unemployment": 7.348,
        "IsHoliday": 1,
        "Size": 151315,
        "Dept": 7
    }'
    sleep $((RANDOM % 5))
done```

View all metrics on Grafana!!

### Notes:

Extract IP Address of Docker Container
```docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' container_name_or_id```










