import pandas as pd
from dag_datalake_sirene.workflows.data_pipelines.egapro.config import EGAPRO_CONFIG


def preprocess_egapro_data(data_dir):
    df_egapro = pd.read_csv(
        EGAPRO_CONFIG.url_minio, dtype={"siren": "object", "egapro_renseignee": "bool"}
    )
    return df_egapro
