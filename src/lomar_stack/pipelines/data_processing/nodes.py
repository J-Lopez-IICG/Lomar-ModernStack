import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, col, lower, trim, count
from functools import reduce

# 1. Instanciamos el logger de Kedro
log = logging.getLogger(__name__)


def unir_multiples_fuentes(**fuentes_datos: DataFrame) -> DataFrame:
    """
    Une N tablas etiquetándolas por su origen.
    """
    lista_dfs = []
    for nombre_origen, df in fuentes_datos.items():
        # Log informativo ligero (Metadata)
        log.info(f"Procesando fuente: {nombre_origen} | Cols: {len(df.columns)}")

        df_etiquetado = df.withColumn("origen_datos", lit(nombre_origen))
        lista_dfs.append(df_etiquetado)

    if not lista_dfs:
        log.error("No se recibieron DataFrames para unir.")
        raise ValueError("Sin tablas para unir")

    log.info(f"Uniendo {len(lista_dfs)} fuentes de datos...")

    return reduce(
        lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), lista_dfs
    )


def limpiar_datos_produccion(data: DataFrame) -> DataFrame:
    """
    Normaliza headers/strings y elimina columnas 100% nulas.
    """
    conteo_inicial_cols = len(data.columns)

    # --- Paso 1: Normalización (Lazy) ---
    expresiones_transformacion = []
    for c, t in data.dtypes:
        nuevo_nombre = c.strip().lower().replace(" ", "_")
        # Aplicamos trim+lower solo si es string
        columna = lower(trim(col(c))) if t == "string" else col(c)
        expresiones_transformacion.append(columna.alias(nuevo_nombre))

    data = data.select(*expresiones_transformacion)

    # --- Paso 2: Detección de Nulos (Action: Trigger de Spark) ---
    log.info("Escaneando columnas vacías (esto dispara una acción de Spark)...")

    expresiones_conteo = [count(c).alias(c) for c in data.columns]
    # .collect() trae los resultados al Driver (memoria local), perfecto para loggear
    conteo_dict = data.agg(*expresiones_conteo).collect()[0].asDict()

    cols_vacias = [
        c for c, total in conteo_dict.items() if total == 0 and c != "origen_datos"
    ]

    # --- Paso 3: Logging de Resultados y Limpieza ---
    if cols_vacias:
        # Usamos WARNING para que destaque en la consola
        log.warning(
            f"Se eliminarán {len(cols_vacias)} columnas 100% VACÍAS: {cols_vacias}"
        )
    else:
        log.info("No se encontraron columnas vacías. Todo el esquema se mantiene.")

    df_limpio = data.drop(*cols_vacias)

    log.info(
        f"Limpieza finalizada. Columnas: {conteo_inicial_cols} -> {len(df_limpio.columns)}"
    )

    return df_limpio
