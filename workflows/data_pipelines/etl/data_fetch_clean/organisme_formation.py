import pandas as pd

from dag_datalake_sirene.workflows.data_pipelines.formation.config import (
    FORMATION_CONFIG,
)


def preprocess_organisme_formation_data(data_dir):
    df_organisme_formation = pd.read_csv(
        FORMATION_CONFIG.url_minio,
        dtype={
            "siren": "object",
            "liste_id_organisme_formation": "object",
            "est_qualiopi": "bool",
        },
    )
    return df_organisme_formation
