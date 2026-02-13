"""Project pipelines."""

from __future__ import annotations
from kedro.pipeline import Pipeline
from .pipelines import data_ingestion as ing
from .pipelines import data_processing as dp
from .pipelines import feature_engineering as fe
from .pipelines import reporting as rep


def register_pipelines() -> dict[str, Pipeline]:
    ingestion_pipe = ing.create_pipeline()
    data_processing_pipe = dp.create_pipeline()
    feature_engineering_pipe = fe.create_pipeline()
    reporting_pipe = rep.create_pipeline()

    # El flujo completo
    default_pipe = (
        ingestion_pipe
        + data_processing_pipe
        + feature_engineering_pipe
        + reporting_pipe
    )

    return {
        "__default__": default_pipe,
        "ingestion": ingestion_pipe,
        "dp": data_processing_pipe,
        "fe": feature_engineering_pipe,
        "reporting": reporting_pipe,
        "prep": data_processing_pipe + feature_engineering_pipe,
    }
