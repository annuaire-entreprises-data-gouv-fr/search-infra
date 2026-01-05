import pandas as pd

from data_pipelines_annuaire.helpers import (
    DataProcessor,
    Notification,
    clean_sirent_column,
)
from data_pipelines_annuaire.workflows.data_pipelines.bilan_ges.config import (
    BILAN_GES_CONFIG,
)


class BilanGesProcessor(DataProcessor):
    def __init__(self):
        super().__init__(BILAN_GES_CONFIG)

    def preprocess_data(self):
        df_bilan_ges = pd.read_csv(
            self.config.files_to_download["bilan_ges"]["url"],
            delimiter=";",
            dtype="str",
            usecols=["SIREN principal"],
        )
        # Clean and transform
        df_bilan_ges = (
            df_bilan_ges.rename(columns={"SIREN principal": "siren"})
            .drop_duplicates(subset=["siren"], keep="first")
            .assign(bilan_ges_renseigne=1)
        )

        # Clean siren column and remove invalid rows
        # Can't add leading zeros because sometimes the Siren are truncated at the end instead like:
        # "3 328 035" for Association Laïque de Gestion d’Etablissements d’E...
        # which real Siren is 332 803 519
        # And some Siren values are empty or completely wrong such as "/"
        df_bilan_ges = clean_sirent_column(
            df_bilan_ges, column_type="siren", max_removal_percentage=0.5
        )

        df_bilan_ges.to_csv(f"{self.config.tmp_folder}/bilan_ges.csv", index=False)

        self.push_message(
            Notification.notification_xcom_key,
            column=df_bilan_ges["siren"],
            description="unités légales",
        )

        del df_bilan_ges
