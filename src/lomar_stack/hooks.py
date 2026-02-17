# src/lomar_stack/hooks.py
from kedro.framework.hooks import hook_impl
from pyspark import SparkConf
from pyspark.sql import SparkSession
import os


class SparkHooks:
    @hook_impl
    def after_context_created(self, context) -> None:
        """Combina el spark.yml con las rutas dinámicas de los JARs"""

        # 1. Cargamos la configuración de forma segura
        # Esto busca en conf/base/spark.yml y devuelve un dict
        try:
            conf_loader = context.config_loader
            conf_params = conf_loader["spark"]
        except Exception:
            conf_params = {}

        project_path = os.getcwd()

        # 2. Definimos las rutas dinámicas
        gcs_jar = f"{project_path}/gcs-connector-hadoop3-latest.jar"
        sql_jar = f"{project_path}/mssql-jdbc-12.8.1.jre11.jar"
        json_key = f"{project_path}/lomar-bibucket-b85f25ba9058.json"

        # 3. Iniciamos la configuración de Spark
        conf = SparkConf()

        # 4. Si conf_params es un diccionario, cargamos sus valores
        if isinstance(conf_params, dict):
            conf.setAll(list(conf_params.items()))

        # 5. Forzamos las rutas críticas (Sobreescribe lo que haya en el yml)
        conf.set("spark.jars", f"{gcs_jar},{sql_jar}")
        conf.set("spark.driver.extraClassPath", f"{gcs_jar}:{sql_jar}")
        conf.set("spark.executor.extraClassPath", f"{gcs_jar}:{sql_jar}")
        conf.set(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile", json_key
        )

        # 6. Creamos la sesión
        SparkSession.builder.config(conf=conf).getOrCreate()
