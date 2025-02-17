import pandas as pd

from dag_datalake_sirene.helpers import DataProcessor, Notification
from dag_datalake_sirene.workflows.data_pipelines.convcollective.config import (
    CONVENTION_COLLECTIVE_CONFIG,
)


class ConventionCollectiveProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__(CONVENTION_COLLECTIVE_CONFIG)

    def preprocess_data(self) -> None:
        df_conv_coll = (
            pd.read_csv(
                self.config.files_to_download["convention_collective"]["destination"],
                dtype="string",
                names=["mois", "siret", "idcc", "date_maj"],
                header=0,
            )
            .assign(
                siren=lambda x: x["siret"].str[0:9],
                idcc=lambda x: x["idcc"].str.replace(" ", ""),
            )
            .dropna(subset=["siret"])
        )

        # List of idcc per siren
        df_list_cc = (
            df_conv_coll.groupby(by=["siren"])["idcc"]
            .unique()
            .apply(list)
            .reset_index(name="liste_idcc_unite_legale")
        )

        # list of idcc per siret
        df_list_cc_per_siret = (
            df_conv_coll.groupby(by=["siret", "siren"])["idcc"]
            .apply(list)
            .reset_index(name="liste_idcc_etablissement")
        )

        # mapping of idcc to the list of relevant siret, per siren
        siret_idcc_dict = {}
        for siren, group in df_conv_coll.groupby("siren"):
            idcc_siret_dict: dict[str, list[str]] = {}
            for _, row in group.iterrows():
                idcc = row["idcc"]
                siret = row["siret"]
                if idcc not in idcc_siret_dict:
                    idcc_siret_dict[idcc] = []
                idcc_siret_dict[idcc].append(siret)
            siret_idcc_dict[siren] = idcc_siret_dict

        df_list_cc_per_siren = pd.DataFrame(
            siret_idcc_dict.items(), columns=["siren", "sirets_par_idcc"]
        )

        df_cc = df_list_cc_per_siret.merge(
            df_list_cc_per_siren, on="siren", how="left"
        ).merge(df_list_cc, on="siren", how="left")

        df_cc.to_csv(self.config.file_output, index=False)

        DataProcessor.push_message(
            Notification.notification_xcom_key,
            column=df_cc.siren,
        )
