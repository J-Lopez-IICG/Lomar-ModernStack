from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from typing import Dict, Any


def generar_kpis_produccion(df: DataFrame, params: Dict[str, Any]) -> DataFrame:
    """
    Realiza conversiones y normalización usando parámetros externos.
    """
    return (
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
