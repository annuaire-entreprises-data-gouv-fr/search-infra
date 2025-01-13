import pandas as pd

from dag_datalake_sirene.workflows.data_pipelines.colter.config import (
    COLTER_CONFIG,
    ELUS_CONFIG,
)


def preprocess_colter_data(data_dir):
    df_colter = pd.read_csv(COLTER_CONFIG.url_minio, dtype=str)
    return df_colter


def preprocess_elus_data(data_dir):
    df_colter_elus = pd.read_csv(ELUS_CONFIG.url_minio, dtype=str)
    return df_colter_elus
