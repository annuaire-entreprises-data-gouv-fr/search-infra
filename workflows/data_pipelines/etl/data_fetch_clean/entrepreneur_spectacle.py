import pandas as pd
from dag_datalake_sirene.config import URL_MINIO_ENTREPRENEUR_SPECTACLE


def preprocess_spectacle_data(data_dir):
    df_spectacle = pd.read_csv(URL_MINIO_ENTREPRENEUR_SPECTACLE, dtype=str)
    return df_spectacle
