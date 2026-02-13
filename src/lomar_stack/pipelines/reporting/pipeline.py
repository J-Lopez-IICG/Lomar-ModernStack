from kedro.pipeline import Pipeline, node, pipeline
from .nodes import create_dim_calendar


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=create_dim_calendar,
                # Usamos la tabla intermedia como "disparador"
                inputs="int_produccion_unificada",
                outputs="rep_dim_calendario",
                name="create_dim_calendar_node",
            ),
        ]
    )
