import logging
from typing import Any, Literal

import pandas as pd

from dag_datalake_sirene.helpers import DataProcessor, Notification
from dag_datalake_sirene.helpers.minio_helpers import File
from dag_datalake_sirene.helpers.utils import flatten_object
from dag_datalake_sirene.workflows.data_pipelines.agence_bio.api import (
    BioApiClient,
)
from dag_datalake_sirene.workflows.data_pipelines.agence_bio.config import (
    AGENCE_BIO_CONFIG,
)


class AgenceBioProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__(AGENCE_BIO_CONFIG)

    @staticmethod
    def get_bio_status(status) -> Literal["valide"] | Literal["invalide"]:
        if status:
            clean_status = [s for s in status if str(s) != "nan"]
            if "ENGAGEE" in clean_status:
                return "valide"
        return "invalide"

    def process_agence_bio_data(
        self, data: list[dict[str, Any]]
    ) -> dict[str, pd.DataFrame]:
        df_bio = pd.DataFrame(data)
        df_bio["flatten_activites"] = df_bio["activites"].apply(
            lambda x: flatten_object(x, "nom")
        )
        df_bio.rename(
            columns={
                "numeroBio": "id_bio",
            },
            inplace=True,
        )

        arr_cert = []
        arr_prod = []
        arr_adr = []
        for _, row in df_bio.iterrows():
            for cert in row["certificats"]:
                dict_cert = cert.copy()
                dict_cert["siret"] = row["siret"]
                dict_cert["id_bio"] = row["id_bio"]
                arr_cert.append(dict_cert)
            for prod in row["productions"]:
                dict_prod = prod.copy()
                dict_prod["siret"] = row["siret"]
                dict_prod["id_bio"] = row["id_bio"]
                arr_prod.append(dict_prod)
            for adr in row["adressesOperateurs"]:
                dict_adr = adr.copy()
                dict_adr["siret"] = row["siret"]
                dict_adr["id_bio"] = row["id_bio"]
                arr_adr.append(dict_adr)

        df_cert = pd.DataFrame(arr_cert)
        df_prod = pd.DataFrame(arr_prod)
        df_adr = pd.DataFrame(arr_adr)

        df_cert = df_cert.filter(
            items=[
                "id_bio",
                "siret",
                "etatCertification",
            ]
        ).rename(
            columns={
                "etatCertification": "etat_certification",
            }
        )

        df_cert = (
            df_cert[df_cert["siret"].str.len() == 14]
            .groupby("siret")
            .agg(liste_id_bio=("id_bio", list), statut_bio=("etat_certification", list))
            .reset_index()
            .assign(
                statut_bio=lambda df: df["statut_bio"].apply(
                    lambda x: self.get_bio_status(x)
                ),
                liste_id_bio=lambda df: df["liste_id_bio"].astype(str),
                siren=lambda df: df["siret"].str[:9],
            )
            .loc[lambda df: df["statut_bio"] == "valide"]
        )

        df_prod = df_prod.dropna(subset=["etatProductions"])
        df_prod["flatten_etatProductions"] = df_prod["etatProductions"].apply(
            lambda x: flatten_object(x, "etatProduction")
        )
        df_prod = df_prod.filter(
            items=[
                "id_bio",
                "siret",
                "code",
                "nom",
                "flatten_etatProductions",
            ]
        ).rename(
            columns={
                "flatten_etatProductions": "etat_productions",
            }
        )

        df_adr = df_adr.filter(
            items=[
                "id_bio",
                "siret",
                "lieu",
                "codePostal",
                "ville",
                "lat",
                "long",
                "codeCommune",
                "active",
                "departementId",
                "typeAdresseOperateurs",
            ]
        ).rename(
            columns={
                "lieu": "adresse",
                "codePostal": "code_postal",
                "ville": "commune",
                "codeCommune": "code_commune",
                "departementId": "code_departement",
                "typeAdresseOperateurs": "type_adresse_operateurs",
            }
        )

        df_bio = df_bio.filter(
            items=[
                "id_bio",
                "siret",
                "raisonSociale",
                "denominationcourante",
                "telephone",
                "email",
                "codeNAF",
                "gerant",
                "dateMaj",
                "telephoneCommerciale",
                "reseau",
                "categories",
                "siteWebs",
                "mixite",
                "flatten_activites",
            ]
        ).rename(
            columns={
                "raisonSociale": "nom_raison_sociale",
                "denominationcourante": "denomination_courante",
                "codeNAF": "code_naf",
                "dateMaj": "date_maj",
                "telephoneCommerciale": "telephone_commerciale",
                "siteWebs": "site_webs",
                "flatten_activites": "activites",
            }
        )

        return {
            "principal": df_bio,
            "certifications": df_cert,
            "productions": df_prod,
            "adresses": df_adr,
        }

    def preprocess_data(self) -> None:
        client = BioApiClient()

        logging.info(msg="Fetching all entries with a département from BIO API...")
        raw_data = client.call_api_bio()
        logging.info(f"Fetched {len(raw_data)} records from BIO API")

        logging.info("Processing BIO API data...")
        processed_data = self.process_agence_bio_data(raw_data)

        # Save to CSV
        for name, df in processed_data.items():
            file_path = f"{self.config.tmp_folder}/agence_bio_{name}.csv"
            df.to_csv(file_path, index=False)
            logging.info(f"Saved {name} data to {file_path}")

        DataProcessor.push_message(
            Notification.notification_xcom_key,
            column=processed_data["principal"]["id_bio"],
            description="identifiants bio",
        )
        DataProcessor.push_message(
            Notification.notification_xcom_key,
            column=processed_data["principal"]["siret"],
            description="siret",
        )

    def send_file_to_minio(self) -> None:
        """Overide of DataProcessor.send_file_to_minio() to manage multiple files."""
        self.minio_client.send_files(
            list_files=[
                File(
                    source_path=f"{self.config.tmp_folder}/",
                    source_name="agence_bio_principal.csv",
                    dest_path=f"{self.config.minio_path}/new/",
                    dest_name="agence_bio_principal.csv",
                    content_type=None,
                ),
                File(
                    source_path=f"{self.config.tmp_folder}/",
                    source_name="agence_bio_certifications.csv",
                    dest_path=f"{self.config.minio_path}/new/",
                    dest_name="agence_bio_certifications.csv",
                    content_type=None,
                ),
                File(
                    source_path=f"{self.config.tmp_folder}/",
                    source_name="agence_bio_productions.csv",
                    dest_path=f"{self.config.minio_path}/new/",
                    dest_name="agence_bio_productions.csv",
                    content_type=None,
                ),
                File(
                    source_path=f"{self.config.tmp_folder}/",
                    source_name="agence_bio_adresses.csv",
                    dest_path=f"{self.config.minio_path}/new/",
                    dest_name="agence_bio_adresses.csv",
                    content_type=None,
                ),
                File(
                    source_path=f"{self.config.tmp_folder}/",
                    source_name="metadata.json",
                    dest_path=f"{self.config.minio_path}/new/",
                    dest_name="metadata.json",
                    content_type=None,
                ),
            ],
        )

    def compare_files_minio(self) -> bool:
        """Overide of DataProcessor.compare_files_minio() to manage multiple files."""
        is_same = self.minio_client.compare_files(
            file_path_1=f"{self.config.minio_path}/new/",
            file_name_2=f"{self.config.file_name}.csv",
            file_path_2=f"{self.config.minio_path}/latest/",
            file_name_1=f"{self.config.file_name}.csv",
        )
        if not is_same:
            self.minio_client.send_files(
                list_files=[
                    File(
                        source_path=f"{self.config.tmp_folder}/",
                        source_name="agence_bio_principal.csv",
                        dest_path=f"{self.config.minio_path}/latest/",
                        dest_name="agence_bio_principal.csv",
                        content_type=None,
                    ),
                    File(
                        source_path=f"{self.config.tmp_folder}/",
                        source_name="agence_bio_certifications.csv",
                        dest_path=f"{self.config.minio_path}/latest/",
                        dest_name="agence_bio_certifications.csv",
                        content_type=None,
                    ),
                    File(
                        source_path=f"{self.config.tmp_folder}/",
                        source_name="agence_bio_productions.csv",
                        dest_path=f"{self.config.minio_path}/latest/",
                        dest_name="agence_bio_productions.csv",
                        content_type=None,
                    ),
                    File(
                        source_path=f"{self.config.tmp_folder}/",
                        source_name="agence_bio_adresses.csv",
                        dest_path=f"{self.config.minio_path}/latest/",
                        dest_name="agence_bio_adresses.csv",
                        content_type=None,
                    ),
                    File(
                        source_path=f"{self.config.tmp_folder}/",
                        source_name="metadata.json",
                        dest_path=f"{self.config.minio_path}/latest/",
                        dest_name="metadata.json",
                        content_type=None,
                    ),
                ],
            )
        return not is_same
