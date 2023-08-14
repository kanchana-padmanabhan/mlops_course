### Dynaconf

https://www.dynaconf.com/


### Configuration can be set up using
* settings.yml - anything non-secret
* .secrets.yml - passwords or other secrets
* environment variables
* hashicorp-vault - External key-value secrets vault


### hashicorp-vault 

```
docker pull hashicorp/vault
docker run -d -e 'VAULT_DEV_ROOT_TOKEN_ID=myroot' -p 8200:8200 container_name_or_id
```

#### Install dynaconf package to use vaults
```
pip install 'dynaconf[vault]
```

#### Export the following environment variables

```
export VAULT_ENABLED_FOR_DYNACONF=true
export VAULT_URL_FOR_DYNACONF="http://localhost:8200"
\# Specify the secrets engine for kv, default is 1
export VAULT_KV_VERSION_FOR_DYNACONF=1
\# Authenticate with token https://www.vaultproject.io/docs/auth/token
export VAULT_TOKEN_FOR_DYNACONF="myroot"
\# Authenticate with root token
export VAULT_ROOT_TOKEN_FOR_DYNACONF="myroot"
```

#### Add secrets to vault using commandline
```
dynaconf -i config.settings write vault -s connection_string=mysql+pymysql://application:passpass@127.0.0.1 -e default
```

#### Add secrets via UI
Go to http://localhost:8200 and login with "Token" -> "myroot"
You can add secrets via UI screen. 


Examples of using dynaconf configs in code can be found in prefect and flyte folders. All the workflows use dynaconf.


### Plain YAML file for configs
* Use the config.yaml file in this folder as an example.
* The utils.py file in prefect/flyte folders have a function load_config that loads a yaml files and returns a dict with key-value pairs. 
* You can use the dict to access the settings
* Pass the config file path using a command line argument or an environment varible

