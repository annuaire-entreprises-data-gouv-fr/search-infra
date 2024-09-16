import pandas as pd
from config import URL_MINIO_EGAPRO


def preprocess_egapro_data(data_dir):
    df_egapro = pd.read_csv(
        URL_MINIO_EGAPRO, dtype={"siren": "object", "egapro_renseignee": "bool"}
    )
    return df_egapro
