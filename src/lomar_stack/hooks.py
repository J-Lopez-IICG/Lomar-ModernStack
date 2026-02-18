# src/lomar_stack/hooks.py
from kedro.framework.hooks import hook_impl
from pyspark import SparkConf
from pyspark.sql import SparkSession
import os


class SparkHooks:
    @hook_impl
    def after_context_created(self, context) -> None:
        """Combina el spark.yml con las rutas dinámicas de los JARs"""

        # 1. Cargamos la configuración de forma segura (spark.yml)
        try:
            conf_loader = context.config_loader
            conf_params = conf_loader["spark"]
        except Exception:
            conf_params = {}

        project_path = os.getcwd()

        # 2. Definimos las rutas dinámicas y de seguridad
        gcs_jar = f"{project_path}/gcs-connector-hadoop3-latest.jar"
        sql_jar = f"{project_path}/mssql-jdbc-12.8.1.jre11.jar"

        # --- Variable de Entorno ---
        env_key = os.environ.get("GCP_KEY_PATH", "lomar-bibucket-b85f25ba9058.json")
        if not os.path.exists(env_key):
            json_key = os.path.join(project_path, os.path.basename(env_key))
        else:
            json_key = env_key

        # 3. Iniciamos la configuración de Spark
        conf = SparkConf()

        # 4. Si conf_params es un diccionario, cargamos sus valores
        if isinstance(conf_params, dict):
            conf.setAll(list(conf_params.items()))

        # 5. Forzamos las rutas críticas
        conf.set("spark.jars", f"{gcs_jar},{sql_jar}")
        conf.set("spark.driver.extraClassPath", f"{gcs_jar}:{sql_jar}")
        conf.set("spark.executor.extraClassPath", f"{gcs_jar}:{sql_jar}")

        # Usamos la variable json_key que ya es inteligente
        conf.set(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile", json_key
        )

        # 6. Creamos la sesión
        SparkSession.builder.config(conf=conf).getOrCreate()
