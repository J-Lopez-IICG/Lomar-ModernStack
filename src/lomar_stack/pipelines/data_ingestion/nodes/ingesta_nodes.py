import logging
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def reportar_produccion_actual(df: DataFrame) -> DataFrame:
    """
    Recibe el DataFrame de Spark desde AWS, imprime un resumen 
    y lo devuelve para el siguiente paso.
    """
    logger = logging.getLogger(__name__)
    
    # Contamos las filas (esto disparará la conexión a AWS)
    count = df.count()
    logger.info(f"📊 Total de registros en produccion_actual: {count}")
    
    # Hacemos una agregación rápida por Especie para ver que los datos son reales
    resumen = df.groupBy("Especie").agg(
        F.sum("KilosNetos").alias("Total_Kilos"),
        F.count("id").alias("Cantidad_Registros")
    )
    
    resumen.show()
    
    return df