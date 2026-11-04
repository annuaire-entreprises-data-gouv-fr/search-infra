import pandas as pd

from data_pipelines_annuaire.helpers import (
    DataProcessor,
    Notification,
    clean_sirent_column,
)
from data_pipelines_annuaire.workflows.data_pipelines.tva.config import TVA_CONFIG


class TvaProcessor(DataProcessor):
    MIN_ROW_COUNT = 4_500_000

    def __init__(self):
        super().__init__(TVA_CONFIG)

    def preprocess_data(self):
        df_tva = pd.read_csv(
            self.config.files_to_download["tva"]["destination"], dtype=str, sep=";"
        )
        if len(df_tva) < self.MIN_ROW_COUNT:
            raise ValueError(
                f"Export TVA incomplet : {len(df_tva)} lignes reçues, "
                f"{self.MIN_ROW_COUNT} attendues au minimum"
            )

        df_tva["siren"] = df_tva["vat_no"].astype(str).str[-9:]
        df_tva["tva_no"] = "FR" + df_tva["vat_no"].astype(str)

        df_tva = (
            df_tva.groupby("siren")["tva_no"]
            .apply(lambda x: list(set(x)))
            .reset_index(name="liste_tva")
        )

        # Clean siren column and remove invalid rows
        df_tva = clean_sirent_column(df_tva, column_type="siren")

        df_tva.to_csv(f"{self.config.tmp_folder}/tva.csv", index=False)

        DataProcessor.push_message(
            Notification.notification_xcom_key,
            column=df_tva["siren"],
            description="unités légales",
        )

        del df_tva
