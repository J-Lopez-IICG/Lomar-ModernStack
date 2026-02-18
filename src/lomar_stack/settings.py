"""Project settings."""

from lomar_stack.hooks import SparkHooks
from kedro.config import OmegaConfigLoader
from omegaconf import OmegaConf
import os

# --- 1. REGISTRO DEL RESOLVER ---
try:
    OmegaConf.register_new_resolver("env", lambda x: os.environ.get(x))
except ValueError:
    pass

# --- 2. CONFIGURACIÓN DEL CARGADOR ---
CONF_SOURCE = "conf"

CONFIG_LOADER_CLASS = OmegaConfigLoader
CONFIG_LOADER_ARGS = {
    "base_env": "base",
    "default_run_env": "local",
}

# --- 3. HOOKS ---
HOOKS = (SparkHooks(),)
