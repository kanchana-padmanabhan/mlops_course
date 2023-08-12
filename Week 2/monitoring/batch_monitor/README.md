### Use command 

```docker-compose  -f docker-compose.yml up```

to spin up the MySQL service in the **statsd** docker network

### Load the sql_dump.sql into the mysql server using the commmand
```docker exec -i container_id_or_name sh -c 'exec mysql -uroot -p"example"' < sql_dump.sql```


### Notes:
* More MySQL docker information can be found here https://hub.docker.com/_/mysql
* Setting up docker-compose with an already defined docker network
https://stackoverflow.com/questions/59547142/why-isnt-my-container-joining-an-existing-network-in-docker-compose