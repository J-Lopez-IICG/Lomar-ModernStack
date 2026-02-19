from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession
from typing import Any
import logging


def create_dim_calendar(_: Any) -> DataFrame:
    """
    Genera un calendario dinámico desde 2025-01-01 hasta el fin del año actual.

    """
    logger = logging.getLogger(__name__)

    # 1. Obtener la sesión activa de Spark
    spark = SparkSession.getActiveSession()
    assert spark is not None

    # 2. Definir el inicio fijo y el fin dinámico
    start_date = "2025-01-01"
    end_date_expr = F.last_day(F.add_months(F.trunc(F.current_date(), "year"), 11))

    # 3. Crear una fila semilla "en el aire" (Memory Only)
    seed_df = spark.createDataFrame([(1,)], ["id"])

    # 4. Generar la secuencia
    df_cal = seed_df.select(
        F.explode(
            F.sequence(
                F.to_date(F.lit(start_date)), end_date_expr, F.expr("interval 1 day")
            )
        ).alias("fecha")
    )

    # 5. Dimensiones de tiempo
    df_cal = (
        df_cal.withColumn("año", F.year(F.col("fecha")))
        .withColumn("mes_nro", F.month(F.col("fecha")))
        .withColumn("nombre_mes", F.date_format(F.col("fecha"), "MMMM"))
        .withColumn("año_mes", F.date_format(F.col("fecha"), "yyyy-MM"))
        .withColumn("orden_mensual", (F.col("año") * 100) + F.col("mes_nro"))
        .withColumn(
            "fecha_proceso_bi",
            F.from_utc_timestamp(F.current_timestamp(), "America/Santiago"),
        )
    )

    logger.info(f"Calendario dinámico generado independientemente desde {start_date}.")

    return df_cal
