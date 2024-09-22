import pandas as pd
from helpers.settings import Settings


def preprocess_finess_data(data_dir):
    df_finess = pd.read_csv(Settings.URL_MINIO_FINESS, dtype=str)
    return df_finess
