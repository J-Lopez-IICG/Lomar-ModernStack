import logging
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def ingestar_raw_data(data: DataFrame) -> DataFrame:
    """
    Realiza la ingesta inicial de los datos desde la fuente (SQL Server).

    En esta etapa de 'Streaming Puro', actuamos como un pasamanos de baja latencia.
    Spark no ejecuta la lectura hasta que una acción sea requerida más adelante,
    manteniendo el plan de ejecución optimizado.

    Args:
        data: DataFrame crudo extraído directamente mediante JDBC.

    Returns:
        DataFrame: El mismo set de datos listo para ser procesado por la capa de integración.
    """
    logger.info(
        "Iniciando ingesta de datos. Esquema detectado con %d columnas.",
        len(data.columns),
    )

    return data
