from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from typing import Dict, Any
from itertools import chain
import re


def generar_kpis_produccion(df: DataFrame, params: Dict[str, Any]) -> DataFrame:
    """
    Enriquecimiento de datos mediante mapeos vectorizados y lógica de negocio.
    Optimiza el uso de memoria evitando joins innecesarios mediante Maps.
    """

    # 1. Normalización de fechas y Flags de Calidad
    # Usamos una sola proyección para evitar múltiples re-escaneos del plan
    df = (
        df.withColumn("fechaproduccion", F.col("fechaproduccion").cast("date"))
        .withColumn(
            "calidad2",
            F.when(
                F.col("calidad") == params["calidad_premium_label"], "premium"
            ).otherwise("no premium"),
        )
        .withColumn(
            "tipoproceso2",
            F.when(F.col("tipoproceso").startswith("re"), "reproceso")
            .when(
                F.col("tipoproceso") == params["proceso_almacenaje_old"],
                params["proceso_almacenaje_new"],
            )
            .otherwise(F.col("tipoproceso")),
        )
    )

    # 2. BAP2: Hash Mapping optimizado (Sustituimos getItem por [])
    if "bap" in df.columns:
        bap_map = params["bap_mapping"]
        # Inyectamos el mapa de parámetros
        bap_expr = F.create_map([F.lit(x) for x in chain(*bap_map.items())])
        # Coalesce asegura que si no hay mapeo, conservemos el valor original
        df = df.withColumn("bap2", F.coalesce(bap_expr[F.col("bap")], F.col("bap")))

    # 3. CATEGORIZACIÓN: Regex Mapping (RLIKE)
    if "producto" in df.columns:
        categories = params["product_categories"]
        # Iniciamos con el valor por defecto
        cat_expr = F.lit("OTRO")

        # Construimos el CASE WHEN de atrás hacia adelante (prioridad)
        for category_name, keywords in reversed(list(categories.items())):
            pattern = "|".join([re.escape(w) for w in keywords])
            cat_expr = F.when(
                F.col("producto").rlike(pattern), F.lit(category_name)
            ).otherwise(cat_expr)

        df = df.withColumn("producto_categoria", cat_expr)

    # 4. WFE: Rendimiento Industrial
    if "presentacionmp" in df.columns:
        yield_map = params["wfe_yield_standards"]
        yield_expr = F.create_map([F.lit(x) for x in chain(*yield_map.items())])
        df = df.withColumn(
            "factor_rendimiento", yield_expr[F.col("presentacionmp")].cast("double")
        )

    return df
