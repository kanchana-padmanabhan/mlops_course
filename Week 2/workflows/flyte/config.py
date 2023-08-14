
from dynaconf import Dynaconf

settings = Dynaconf(
    environments=True,
    envvar_prefix="CUSTOM",
    settings_files=['settings.yaml', '.secrets.yaml'],
    vault_enabled=True
)

# `envvar_prefix` = export envvars with `export DYNACONF_FOO=bar`.
# `settings_files` = Load these files in the order.
