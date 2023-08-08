## Welcome to Week 1 of Model Deployment

### Set up

### Machine info (mine)
Apple M2 Pro, 16GB RAM, Ventura 13.3.1

#### Set up VirtualEnv
virtualenv venv
source venv/bin/activate

#### Teardown virtual env
deactivate

#### Env variables

Provide some defaults in env_variables.sh
source env_variables.sh (to set up variables in your env)

#### Install MySQL database
https://dev.mysql.com/doc/mysql-installation-excerpt/5.7/en/macos-installation-notes.html

I set up a username "application" and password "passpass"

#### Install Docker
https://docs.docker.com/engine/install/
Make sure your docker deamon is running. 


#### Python Dependencies (Python 3.8.17)
pip install -r requirements.txt


#### Run jupyter
nohup jupyter notebook &

#### Start mlflow (Two ways)
mlflow server 
(see docs to specify backends if needed - https://mlflow.org/docs/latest/cli.html)
mlflow ui (good for local build)




#### Useful debugging links
* https://stackoverflow.com/questions/7927854/how-to-start-mysql-server-from-command-line-on-mac-os-lion
* https://stackoverflow.com/questions/10299148/mysql-error-1045-28000-access-denied-for-user-billlocalhost-using-passw
* https://stackoverflow.com/questions/17157721/how-to-get-a-docker-containers-ip-address-from-the-host
* https://stackoverflow.com/questions/39597925/how-do-i-set-environment-variables-during-the-build-in-docker
* https://stackoverflow.com/questions/51079061/unable-to-access-to-mlflow-ui
* https://pythontic.com/pandas/serialization/mysql
* https://crontab.guru/every-2-minutes
* https://serverfault.com/questions/449651/why-is-my-crontab-not-working-and-how-can-i-troubleshoot-it







