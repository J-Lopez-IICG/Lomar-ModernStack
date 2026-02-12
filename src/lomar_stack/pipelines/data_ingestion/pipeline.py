from kedro.pipeline import Pipeline, node, pipeline
from .nodes import ingestar_raw_data


def create_pipeline(**kwargs) -> Pipeline:
    # Nombres que coincidan con lo que pusimos en catalog (la parte del medio)
    # Ejemplo: si en catalog es "sql_primar_produccion", aquí pones "primar_produccion"
    origenes_datos = ["primar_produccion", "pda_produccion", "patsac_produccion"]

    nodos_ingesta = []

    for origen in origenes_datos:
        nodos_ingesta.append(
            node(
                func=ingestar_raw_data,
                inputs=f"sql_{origen}",  # Buscará sql_primar_produccion, etc.
                outputs=f"raw_{origen}",  # Creará raw_primar_produccion, etc.
                name=f"ingest_{origen}_node",
            )
        )

    return pipeline(nodos_ingesta)
