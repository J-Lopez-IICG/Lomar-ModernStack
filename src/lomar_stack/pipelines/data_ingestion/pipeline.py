from kedro.pipeline import Pipeline, node, pipeline
from .nodes import ingestar_raw_data


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=ingestar_raw_data,  # La función genérica
                inputs="sql_produccion_actual",  # El dataset específico del catálogo
                outputs="raw_produccion_actual",  # Dónde se guardará
                name="ingest_produccion_node",  # Nombre del paso en el proceso
            ),
        ]
    )
