from kedro.pipeline import Pipeline, node, pipeline
from .nodes import ingestar_raw_data


def create_pipeline(**kwargs) -> Pipeline:
    origenes_datos = ["primar_produccion", "pda_produccion", "patsac_produccion"]

    nodos_ingesta = []

    for origen in origenes_datos:
        nodos_ingesta.append(
            node(
                func=ingestar_raw_data,
                inputs=f"sql_{origen}",
                outputs=f"raw_{origen}",
                name=f"ingest_{origen}_node",
            )
        )

    return pipeline(nodos_ingesta)
