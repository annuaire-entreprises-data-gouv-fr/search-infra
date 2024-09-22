import pandas as pd
from helpers.settings import Settings


def preprocess_convcollective_data(data_dir):
    df_cc = pd.read_csv(
        Settings.URL_MINIO_CONVENTION_COLLECTIVE,
        dtype=str,
    )
    return df_cc
