import pandas as pd
from dag_datalake_sirene.workflows.data_pipelines.convcollective.config import (
    CONVENTION_COLLECTIVE_CONFIG,
)


def preprocess_convcollective_data(data_dir):
    df_cc = pd.read_csv(
        CONVENTION_COLLECTIVE_CONFIG.url_minio,
        dtype=str,
    )
    return df_cc
