"""Project settings."""

from lomar_stack.hooks import SparkHooks
from kedro.config import OmegaConfigLoader
from omegaconf import OmegaConf
import os

# --- 1. REGISTRO DEL RESOLVER ---
# Permite usar ${env:VARIABLE} en tus YAML
try:
    OmegaConf.register_new_resolver("env", lambda x: os.environ[x])
except ValueError:
    pass

# --- 2. CONFIGURACIÓN DEL CARGADOR ---
CONF_SOURCE = "conf"

CONFIG_LOADER_CLASS = OmegaConfigLoader
CONFIG_LOADER_ARGS = {
    "base_env": "base",
    "default_run_env": "local",
    "config_patterns": {
        "spark": ["spark*"],  # Busca spark.yml
        "parameters": ["parameters*"],  # Busca parameters.yml
        "catalog": ["catalog*"],  # Busca catalog.yml
    },
}

# --- 3. HOOKS ---
HOOKS = (SparkHooks(),)
