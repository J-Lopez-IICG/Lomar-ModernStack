from pyspark.sql import functions as F
from pyspark.sql import DataFrame
import logging


def create_dim_calendar(data: DataFrame) -> DataFrame:
    """
    Genera un calendario dinámico desde 2025-01-01 hasta el fin del año actual.
    Optimizado para Spark sin necesidad de pasarle una columna de anclaje.
    """
    logger = logging.getLogger(__name__)

    # 1. Definir el inicio fijo
    start_date = "2025-01-01"

    # 2. Definir el fin dinámico (31 de diciembre del año actual)
    # Esto hace que el calendario crezca solo cada 1 de enero
    end_date_expr = F.last_day(F.add_months(F.trunc(F.current_date(), "year"), 11))

    # 3. Generar la secuencia (usamos una fila de semilla para disparar la generación)
    df_cal = data.limit(1).select(
        F.explode(
            F.sequence(
                F.to_date(F.lit(start_date)), end_date_expr, F.expr("interval 1 day")
            )
        ).alias("fecha")
    )

    # 4. Dimensiones de tiempo vectorizadas para Power BI
    df_cal = (
        df_cal.withColumn("año", F.year(F.col("fecha")))
        .withColumn("mes_nro", F.month(F.col("fecha")))
        .withColumn("nombre_mes", F.date_format(F.col("fecha"), "MMMM"))
        .withColumn("año_mes", F.date_format(F.col("fecha"), "yyyy-MM"))
        .withColumn("orden_mensual", (F.col("año") * 100) + F.col("mes_nro"))
    )

    # Log para confirmar en consola
    logger.info(f"Calendario dinámico generado desde {start_date} en adelante.")

    return df_cal
