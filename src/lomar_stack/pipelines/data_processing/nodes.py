import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, col, lower, trim, count
from pyspark import StorageLevel
from functools import reduce

log = logging.getLogger(__name__)


def unir_multiples_fuentes(**fuentes_datos: DataFrame) -> DataFrame:
    """
    Une múltiples fuentes JDBC aplicando un etiquetado de origen dinámico.
    Utiliza un enfoque funcional (reduce) para evitar recursión pesada.
    """
    if not fuentes_datos:
        log.error("Diccionario de fuentes vacío.")
        raise ValueError("Se requieren DataFrames para realizar la unión.")

    lista_dfs = []
    for nombre_origen, df in fuentes_datos.items():
        log.info(
            f"Integrando fuente: {nombre_origen.upper()} | Dimensiones iniciales: {len(df.columns)} columnas"
        )
        # Inyectamos el origen para trazabilidad en capas superiores (Silver/Gold)
        lista_dfs.append(df.withColumn("origen_datos", lit(nombre_origen)))

    # unionByName es preferible a union() porque ignora el orden de las columnas de SQL Server
    return reduce(
        lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), lista_dfs
    )


def limpiar_datos_produccion(data: DataFrame) -> DataFrame:
    """
    Normaliza el catálogo de columnas y purga dimensiones sin datos.
    Optimiza el rendimiento mediante persistencia en memoria/disco.
    """
    # --- PASO 1: Persistencia Estratégica ---
    # Como vamos a contar nulos (acción 1) y luego guardar el resultado (acción 2),
    # persistimos para no leer SQL Server dos veces.
    data.persist(StorageLevel.MEMORY_AND_DISK)

    conteo_inicial_cols = len(data.columns)

    # --- PASO 2: Normalización de Esquema (Mapping Lazy) ---
    expresiones = [
        (lower(trim(col(c))) if t == "string" else col(c)).alias(
            c.strip().lower().replace(" ", "_")
        )
        for c, t in data.dtypes
    ]
    data = data.select(*expresiones)

    # --- PASO 3: Auditoría de Calidad (Action) ---
    log.info("Iniciando escaneo de vacíos mediante agregación paralela...")

    # Optimizamos el conteo trayendo una sola fila al Driver
    res_conteo = data.agg(*[count(c).alias(c) for c in data.columns]).collect()[0]
    conteo_dict = res_conteo.asDict()

    cols_vacias = [
        c for c, total in conteo_dict.items() if total == 0 and c != "origen_datos"
    ]

    if cols_vacias:
        log.warning(
            f"Data Quality Alert: Eliminando {len(cols_vacias)} columnas nulas: {cols_vacias}"
        )

    df_limpio = data.drop(*cols_vacias)

    log.info(
        f"Proceso de limpieza completado: {conteo_inicial_cols} -> {len(df_limpio.columns)} columnas."
    )

    # Liberamos la memoria una vez que el plan de ejecución está definido
    data.unpersist()

    return df_limpio
