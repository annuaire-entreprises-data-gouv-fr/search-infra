import pandas as pd
from config import URL_MINIO_CONVENTION_COLLECTIVE


def preprocess_convcollective_data(data_dir):
    df_cc = pd.read_csv(
        URL_MINIO_CONVENTION_COLLECTIVE,
        dtype=str,
    )
    return df_cc
