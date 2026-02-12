from kedro.pipeline import Pipeline, node, pipeline
from .nodes import unir_multiples_fuentes, limpiar_datos_produccion


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=unir_multiples_fuentes,
                inputs={
                    "chile": "raw_primar_produccion",
                    "argentina": "raw_pda_produccion",
                    "peru": "raw_patsac_produccion",
                },
                outputs="pre_produccion_unida",
                name="unir_fuentes_node",
            ),
            node(
                func=limpiar_datos_produccion,
                inputs="pre_produccion_unida",
                outputs="int_produccion_unificada",
                name="limpiar_produccion_node",
            ),
        ]
    )
