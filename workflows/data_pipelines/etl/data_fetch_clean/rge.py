import pandas as pd
from helpers.settings import Settings


def preprocess_rge_data(data_dir):
    df_rge = pd.read_csv(Settings.URL_MINIO_RGE, dtype=str)
    return df_rge
