docker build -t statsd .
docker run -p 8125:8125/udp -p 8126:8126/tcp statsd 
