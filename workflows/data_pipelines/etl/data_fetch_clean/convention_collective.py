import pandas as pd
from dag_datalake_sirene.config import URL_MINIO_CONVENTION_COLLECTIVE


def preprocess_convcollective_data(data_dir, **kwargs):
    df_cc = pd.read_csv(
        URL_MINIO_CONVENTION_COLLECTIVE,
        dtype=str,
    )
    return df_cc
