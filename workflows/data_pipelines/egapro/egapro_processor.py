import pandas as pd
import logging
from dag_datalake_sirene.helpers.data_processor import DataProcessor
from dag_datalake_sirene.workflows.data_pipelines.egapro.config import EGAPRO_CONFIG


class EgaproProcessor(DataProcessor):
    def __init__(self):
        super().__init__(EGAPRO_CONFIG)

    def preprocess_data(self, ti):
        df_egapro = pd.read_excel(
            self.config.url,
            dtype=str,
            engine="openpyxl",
        )
        df_egapro = df_egapro.drop_duplicates(subset=["SIREN"], keep="first")
        df_egapro = df_egapro[["SIREN"]]
        df_egapro["egapro_renseignee"] = True
        df_egapro = df_egapro.rename(columns={"SIREN": "siren"})
        df_egapro.to_csv(f"{self.config.tmp_folder}/egapro.csv", index=False)

        unique_count = df_egapro["siren"].nunique()
        ti.xcom_push(key="nb_siren_egapro", value=str(unique_count))

        logging.info(f"Processed {unique_count} unique SIREN values.")

        del df_egapro

    def send_notification(self, ti):
        nb_siren = ti.xcom_pull(key="nb_siren_egapro", task_ids="process_egapro")
        super().send_notification(
            f"\U0001f7e2 Données Egapro mises à jour.\n"
            f"- {nb_siren} unités légales représentées.",
        )
