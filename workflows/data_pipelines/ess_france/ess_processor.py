import pandas as pd
from dag_datalake_sirene.helpers import Notification, DataProcessor
from dag_datalake_sirene.workflows.data_pipelines.ess_france.config import ESS_CONFIG
from airflow.operators.python import get_current_context


class EssFranceProcessor(DataProcessor):
    def __init__(self):
        super().__init__(ESS_CONFIG)

    def preprocess_data(self):
        df_ess = pd.read_csv(self.config.url, dtype=str)
        df_ess["SIREN"] = df_ess["SIREN"].str.zfill(9)
        df_ess.rename(columns={"SIREN": "siren"}, inplace=True)
        df_ess["est_ess_france"] = True
        df_ess = df_ess[["siren", "est_ess_france"]]

        df_ess.to_csv(f"{self.config.tmp_folder}/ess.csv", index=False)

        DataProcessor._push_unique_count(df_ess["siren"], "nb_siren_ess")
        del df_ess

    def send_file_to_minio(self):
        super().send_file_to_minio()
        ti = get_current_context()["ti"]
        nb_siren = ti.xcom_pull(key="nb_siren_ess", task_ids="preprocess_ess_france")
        message = f"{nb_siren} unités légales"
        ti.xcom_push(key=Notification.notification_xcom_key, value=message)
