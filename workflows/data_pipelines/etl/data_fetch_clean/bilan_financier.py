import pandas as pd
from dag_datalake_sirene.helpers.utils import get_last_modified
from dag_datalake_sirene.config import URL_BILANS_FINANCIERS


def preprocess_bilan_financier_data(data_dir, **kwargs):
    df_bilan = pd.read_csv(URL_BILANS_FINANCIERS, dtype=str)
    df_bilan["ca"] = df_bilan["ca"].astype(float)
    df_bilan["resultat_net"] = df_bilan["resultat_net"].astype(float)

    # Get the last modified date of the CSV file
    last_modified = get_last_modified(URL_BILANS_FINANCIERS)
    kwargs["ti"].xcom_push(key="bilan_financier_last_modified", value=last_modified)

    return df_bilan
