import pandas as pd
from dag_datalake_sirene.workflows.data_pipelines.ess_france.config import (
    ESS_CONFIG,
)


def preprocess_ess_france_data(data_dir):
    df_ess = pd.read_csv(
        ESS_CONFIG.url_minio,
        dtype={
            "siren": "object",
            "est_ess_france": "bool",
        },
    )
    return df_ess
