1. Follow instructions on https://docs.prefect.io/2.11.3/guides/deployment/docker/ (use the example in the folder)
2. Remember to start the workpool workers before trying to run any deployment
```prefect worker start --pool above-ground_v2 --work-queue default
```
3. "Process" workpools work best when tetsing locally using docker containers
4. use "from prefect import getlogger" to log and view any outputs as part of the log files in the UI