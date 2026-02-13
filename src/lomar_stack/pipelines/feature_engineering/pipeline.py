from kedro.pipeline import Pipeline, node, pipeline
from .nodes import generar_kpis_produccion


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=generar_kpis_produccion,
                inputs=["int_produccion_unificada", "params:produccion"],
                outputs="prm_produccion_enriquecida",
                name="generar_kpis_produccion_node",
            ),
        ]
    )
