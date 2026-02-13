from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from typing import Dict, Any
from itertools import chain
import re  # Usamos re.escape para optimizar la búsqueda de texto


def generar_kpis_produccion(df: DataFrame, params: Dict[str, Any]) -> DataFrame:
    """
    Nodo optimizado para la creación de KPIs y categorización estratégica.
    """
    # 1. KPIs Básicos de Tonelaje y Calidad
    df = (
        df.withColumn("tons_netos", F.col("kilosnetos") / params["conversion_ton"])
        .withColumn("tons_brutos", F.col("kilosbrutos") / params["conversion_ton"])
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

    # 2. BAP2: Optimización mediante Hash Mapping ( create_map )
    if "bap" in df.columns:
        bap_map = params["bap_mapping"]
        # El mapeo se inyecta como una sola operación de Spark
        bap_expr = F.create_map([F.lit(x) for x in chain(*bap_map.items())])
        df = df.withColumn(
            "bap2", F.coalesce(bap_expr.getItem(F.col("bap")), F.col("bap"))
        )

    # 3. CATEGORIZACIÓN: Optimización mediante Regex Vectorizado ( rlike )
    if "producto" in df.columns:
        categories = params["product_categories"]
        expr = F.lit("OTRO")

        # Construimos un solo bloque CASE WHEN dinámico
        for category_name, keywords in reversed(list(categories.items())):
            # pattern = "palabra1|palabra2" -> Búsqueda ultra rápida en el motor de Spark
            pattern = "|".join([re.escape(w) for w in keywords])
            expr = F.when(F.col("producto").rlike(pattern), category_name).otherwise(
                expr
            )

        df = df.withColumn("producto_categoria", expr)

    # 4. WFE: Cálculo de rendimiento basado en estándares industriales
    if "presentacionmp" in df.columns:
        yield_map = params["wfe_yield_standards"]
        mapping_expr = F.create_map([F.lit(x) for x in chain(*yield_map.items())])

        df = df.withColumn(
            "factor_rendimiento", mapping_expr.getItem(F.col("presentacionmp"))
        )
        df = df.withColumn(
            "tons_wfe",
            F.when(
                F.col("factor_rendimiento") > 0,
                F.col("tons_netos") / F.col("factor_rendimiento"),
            ).otherwise(F.lit(0)),
        )
    df = df.withColumn("fecha_proceso_bi", F.current_timestamp())
    return df
