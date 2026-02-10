from kedro.pipeline import Pipeline, node, pipeline
from .nodes.ingesta_nodes import reportar_produccion_actual

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=reportar_produccion_actual,
                inputs="produccion_actual",  # Nombre que pusimos en el catalog.yml
                outputs="produccion_procesada",
                name="nodo_reporte_aws",
            ),
        ]
    )