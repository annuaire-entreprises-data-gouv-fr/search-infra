import pandas as pd

from dag_datalake_sirene.workflows.data_pipelines.spectacle.spectacle_config import (
    SPECTACLE_CONFIG,
)


def preprocess_spectacle_data(data_dir):
    df_spectacle = pd.read_csv(
        SPECTACLE_CONFIG.url_minio,
        dtype={
            "siren": "object",
            "statut_entrepreneur_spectacle": "object",
            "est_entrepreneur_spectacle": "bool",
        },
    )

    return df_spectacle
