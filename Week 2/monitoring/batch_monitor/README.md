### Use command 

```docker-compose  -f docker-compose.yml up```

to spin up the MySQL service in the **statsd** docker network

### Load the sql_dump.sql into the mysql server using the commmand
```docker exec -i container_id_or_name sh -c 'exec mysql -uroot -p"example"' < sql_dump.sql```

### Monitor and Analyze model performance using Grafana
* Go to the Grafana console at http://localhost:3000/?orgId=1 (see inference_monitor folder to set up)
* Add new datasource and choose MySQL
* Get MySQL container IP address and port 3306.
* Use the username root and password as example (as set up in the MySQL dockerfile)
* database as **retail_dataset_kaggle**
* You can now "Explore" the database and tables or create a dashboard/visualizations to monitor your batchjob



### Notes:
* More MySQL docker information can be found here https://hub.docker.com/_/mysql
* Setting up docker-compose with an already defined docker network
https://stackoverflow.com/questions/59547142/why-isnt-my-container-joining-an-existing-network-in-docker-compose