import pandas as pd
from config import URL_MINIO_FINESS


def preprocess_finess_data(data_dir):
    df_finess = pd.read_csv(URL_MINIO_FINESS, dtype=str)
    return df_finess
