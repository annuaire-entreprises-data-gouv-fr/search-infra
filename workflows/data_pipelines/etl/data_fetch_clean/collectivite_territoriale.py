import pandas as pd
from helpers.settings import Settings


def preprocess_colter_data(data_dir):
    df_colter = pd.read_csv(Settings.URL_MINIO_COLTER, dtype=str)
    return df_colter


def preprocess_elus_data(data_dir):
    df_colter_elus = pd.read_csv(Settings.URL_MINIO_ELUS, dtype=str)
    return df_colter_elus
