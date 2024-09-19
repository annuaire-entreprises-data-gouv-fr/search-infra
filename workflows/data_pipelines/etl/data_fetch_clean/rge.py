import pandas as pd
from config import URL_MINIO_RGE


def preprocess_rge_data(data_dir):
    df_rge = pd.read_csv(URL_MINIO_RGE, dtype=str)
    return df_rge
