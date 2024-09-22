import pandas as pd
from helpers.settings import Settings


def preprocess_egapro_data(data_dir):
    df_egapro = pd.read_csv(
        Settings.URL_MINIO_EGAPRO, dtype={"siren": "object", "egapro_renseignee": "bool"}
    )
    return df_egapro
