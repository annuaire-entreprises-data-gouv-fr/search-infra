import pandas as pd
from dag_datalake_sirene.workflows.data_pipelines.rge.config import RGE_CONFIG


def preprocess_rge_data(data_dir):
    df_rge = pd.read_csv(RGE_CONFIG.url_minio, dtype=str)
    return df_rge
