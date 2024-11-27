import pandas as pd
from dag_datalake_sirene.workflows.data_pipelines.finess.config import FINESS_CONFIG


def preprocess_finess_data(data_dir):
    df_finess = pd.read_csv(FINESS_CONFIG.url_minio, dtype=str)
    return df_finess
