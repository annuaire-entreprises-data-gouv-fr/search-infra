import pandas as pd
from dag_datalake_sirene.config import URL_MINIO_ESS_FRANCE


def preprocess_ess_france_data(data_dir):
    df_ess = pd.read_csv(URL_MINIO_ESS_FRANCE, dtype=str)
    return df_ess
