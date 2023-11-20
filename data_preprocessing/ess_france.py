import pandas as pd
from dag_datalake_sirene.config import URL_ESS_FRANCE


def preprocess_ess_france_data(data_dir):
    df_ess = pd.read_csv(URL_ESS_FRANCE, dtype=str)
    df_ess["SIREN"] = df_ess["SIREN"].str.zfill(9)
    df_ess.rename(columns={"SIREN": "siren"}, inplace=True)
    df_ess["est_ess"] = True
    return df_ess
