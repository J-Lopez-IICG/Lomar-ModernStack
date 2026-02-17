from kedro.pipeline import Pipeline, node, pipeline
from .nodes import create_dim_calendar


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=create_dim_calendar,
                # Usamos un parámetro como disparador. Esto rompe la dependencia directa con tablas de producción
                inputs="params:create_calendar_trigger",
                outputs="rep_dim_calendario",
                name="create_dim_calendar_node",
            ),
        ]
    )
