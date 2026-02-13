"""Project settings."""

from kedro.config import OmegaConfigLoader
from omegaconf import OmegaConf
import os

# --- 1. REGISTRO DEL RESOLVER (El Truco Maestro) ---
# Esto le enseña a Kedro a entender la sintaxis ${env:NOMBRE_VARIABLE}
# Si la variable no existe, devolverá None en lugar de explotar.
try:
    OmegaConf.register_new_resolver("env", lambda x: os.environ.get(x))
except ValueError:
    # Si ya existe (por si reinicias el kernel), lo ignoramos
    pass

# --- 2. CONFIGURACIÓN DEL CARGADOR ---
CONF_SOURCE = "conf"

CONFIG_LOADER_CLASS = OmegaConfigLoader
CONFIG_LOADER_ARGS = {
    "base_env": "base",
    "default_run_env": "local",
}

# --- 3. HOOKS ---
HOOKS = ()
