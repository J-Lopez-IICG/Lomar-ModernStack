import logging
from pyspark.sql import DataFrame


def ingestar_raw_data(data: DataFrame) -> DataFrame:
    """
    Versión Ultra-Rápida (Streaming Puro).
    """
    return data
