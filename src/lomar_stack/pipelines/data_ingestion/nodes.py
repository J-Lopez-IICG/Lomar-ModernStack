import logging
from pyspark.sql import DataFrame


def ingestar_raw_data(data: DataFrame) -> DataFrame:
    """
    Nodo genérico de ingestión.
    1. Recibe un DataFrame (cualquiera).
    2. Cuenta los registros (trigger de acción en Spark).
    3. Devuelve el DataFrame para que Kedro lo guarde.
    """
    # Usamos el logger de Kedro para que se vea bonito en la terminal
    log = logging.getLogger(__name__)

    # Acción de contar
    total_registros = data.count()

    log.info(
        f"✅ Ingestión completada. Total de registros procesados: {total_registros}"
    )

    # Devolvemos la data intacta para que se guarde en 01_raw
    return data
