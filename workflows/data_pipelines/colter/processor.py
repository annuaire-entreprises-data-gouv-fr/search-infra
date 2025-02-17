import logging
import zipfile

import pandas as pd
import requests

from dag_datalake_sirene.helpers import DataProcessor
from dag_datalake_sirene.helpers.utils import get_current_year
from dag_datalake_sirene.workflows.data_pipelines.colter.config import (
    COLTER_CONFIG,
    ELUS_CONFIG,
)


class ColterProcessor(DataProcessor):
    def __init__(self):
        super().__init__(COLTER_CONFIG)

    def preprocess_data(self):
        # Process Régions
        df_regions = pd.read_csv(
            self.config.files_to_download["colter_regions"]["url"],
            dtype=str,
            sep=";",
            usecols=["Code Insee 2023 Région", "Code Siren Collectivité", "Exercice"],
        )
        df_regions = df_regions.rename(
            columns={
                "Code Insee 2023 Région": "colter_code_insee",
                "Code Siren Collectivité": "siren",
                "Exercice": "exercice",
            }
        )

        df_regions = df_regions[df_regions["exercice"] == df_regions.exercice.max()][
            ["colter_code_insee", "siren"]
        ]
        df_regions = df_regions.drop_duplicates(keep="first")
        df_regions["colter_code"] = df_regions["colter_code_insee"]
        df_regions["colter_niveau"] = "region"

        # Cas particulier Corse
        df_regions.loc[df_regions["colter_code_insee"] == "94", "colter_niveau"] = (
            "particulier"
        )
        df_colter = df_regions

        # Process Départements
        df_deps = pd.read_csv(
            self.config.files_to_download["colter_deps"]["url"],
            dtype=str,
            sep=";",
            usecols=[
                "Code Insee 2023 Département",
                "Code Siren Collectivité",
                "Exercice",
            ],
        )
        df_deps = df_deps.rename(
            columns={
                "Code Insee 2023 Département": "colter_code_insee",
                "Code Siren Collectivité": "siren",
                "Exercice": "exercice",
            }
        )

        df_deps = df_deps[df_deps["exercice"] == df_deps["exercice"].max()]
        df_deps = df_deps[["colter_code_insee", "siren"]]
        df_deps = df_deps.drop_duplicates(keep="first")
        df_deps["colter_code"] = df_deps["colter_code_insee"] + "D"
        df_deps["colter_niveau"] = "departement"

        # Cas Métropole de Lyon
        df_deps.loc[df_deps["colter_code_insee"] == "691", "colter_code"] = "69M"
        df_deps.loc[df_deps["colter_code_insee"] == "691", "colter_niveau"] = (
            "particulier"
        )
        df_deps.loc[df_deps["colter_code_insee"] == "691", "colter_code_insee"] = None

        # Cas Conseil départemental du Rhone
        df_deps.loc[df_deps["colter_code_insee"] == "69", "colter_niveau"] = (
            "particulier"
        )
        df_deps.loc[df_deps["colter_code_insee"] == "69", "colter_code_insee"] = None

        # Cas Collectivité Européenne d"Alsace
        df_deps.loc[df_deps["colter_code_insee"] == "67A", "colter_code"] = "6AE"
        df_deps.loc[df_deps["colter_code_insee"] == "67A", "colter_niveau"] = (
            "particulier"
        )
        df_deps.loc[df_deps["colter_code_insee"] == "67A", "colter_code_insee"] = None

        # Remove Paris
        df_deps = df_deps[df_deps["colter_code_insee"] != "75"]

        df_colter = pd.concat([df_colter, df_deps])

        # Process EPCI
        url_colter_epci = ColterProcessor.get_epci_url()
        df_epci = pd.read_excel(url_colter_epci, dtype=str, engine="openpyxl")
        df_epci["colter_code_insee"] = None
        df_epci["colter_code"] = df_epci["siren"]
        df_epci["colter_niveau"] = "epci"
        df_epci = df_epci[
            ["colter_code_insee", "siren", "colter_code", "colter_niveau"]
        ]
        df_colter = pd.concat([df_colter, df_epci])

        # Process Communes
        response = requests.get(self.config.files_to_download["colter_communes"]["url"])
        open(f"{self.config.tmp_folder}siren-communes.zip", "wb").write(
            response.content
        )

        with zipfile.ZipFile(
            f"{self.config.tmp_folder}siren-communes.zip", "r"
        ) as zip_ref:
            zip_ref.extractall(f"{self.config.tmp_folder}siren-communes")

        df_communes = pd.read_excel(
            f"{self.config.tmp_folder}siren-communes/Banatic_SirenInsee2022.xlsx",
            dtype=str,
            engine="openpyxl",
        )
        df_communes["colter_code_insee"] = df_communes["insee"]
        df_communes["colter_code"] = df_communes["insee"]
        df_communes["colter_niveau"] = "commune"
        df_communes = df_communes[
            ["colter_code_insee", "siren", "colter_code", "colter_niveau"]
        ]
        df_communes.loc[df_communes["colter_code_insee"] == "75056", "colter_code"] = (
            "75C"
        )
        df_communes.loc[
            df_communes["colter_code_insee"] == "75056", "colter_niveau"
        ] = "particulier"

        df_colter = pd.concat([df_colter, df_communes])
        df_colter.to_csv(self.config.file_output, index=False)

    @staticmethod
    def generate_epci_url(year):
        return (
            "https://www.collectivites-locales.gouv.fr/"
            f"files/Accueil/DESL/{year}/epcicom{year}.xlsx"
        )

    @staticmethod
    def get_epci_url():
        current_year = get_current_year()
        url = ColterProcessor.generate_epci_url(current_year)
        try:
            response = requests.head(url)
            if response.status_code == 200:
                return url
            else:
                logging.error(f"url: {url} returns error!!! ")
                previous_year = current_year - 1

                return ColterProcessor.generate_epci_url(previous_year)
        except requests.RequestException as e:
            raise e


class ElusProcessor(DataProcessor):
    def __init__(self):
        super().__init__(ELUS_CONFIG)

    def preprocess_data(self):
        df_colter = pd.read_csv(COLTER_CONFIG.url_minio, dtype=str)
        # Conseillers régionaux
        elus = self.process_elus_files(
            self.config.files_to_download["conseillers_regionaux"]["url"],
            "Code de la région",
        )

        # Conseillers départementaux
        df_elus_deps = self.process_elus_files(
            self.config.files_to_download["conseillers_departementaux"]["url"],
            "Code du département",
        )
        df_elus_deps["colter_code"] = df_elus_deps["colter_code"] + "D"
        df_elus_deps.loc[df_elus_deps["colter_code"] == "6AED", "colter_code"] = "6AE"
        elus = pd.concat([elus, df_elus_deps])

        # membres des assemblées des collectivités à statut particulier
        df_elus_part = self.process_elus_files(
            self.config.files_to_download["assemblee_col_statut_particulier"]["url"],
            "Code de la collectivité à statut particulier",
        )
        df_elus_part.loc[df_elus_part["colter_code"] == "972", "colter_code"] = "02"
        df_elus_part.loc[df_elus_part["colter_code"] == "973", "colter_code"] = "03"
        elus = pd.concat([elus, df_elus_part])

        # Conseillers communautaires
        df_elus_epci = self.process_elus_files(
            self.config.files_to_download["elus_epci"]["url"],
            "N° SIREN",
        )
        elus = pd.concat([elus, df_elus_epci])

        # Conseillers municipaux
        df_elus_epci = self.process_elus_files(
            self.config.files_to_download["conseillers_municipaux"]["url"],
            "Code de la commune",
        )
        df_elus_epci.loc[df_elus_epci["colter_code"] == "75056", "colter_code"] = "75C"
        elus = pd.concat([elus, df_elus_epci])
        df_colter_elus = elus.merge(df_colter, on="colter_code", how="left")
        df_colter_elus = df_colter_elus[df_colter_elus["siren"].notna()]
        df_colter_elus = df_colter_elus[
            [
                "siren",
                "nom_elu",
                "prenom_elu",
                "date_naissance_elu",
                "sexe_elu",
                "fonction_elu",
            ]
        ]
        for col in df_colter_elus.columns:
            df_colter_elus = df_colter_elus.rename(
                columns={col: col.replace("_elu", "")}
            )

        df_colter_elus.drop_duplicates(inplace=True, ignore_index=True)
        df_colter_elus.to_csv(self.config.file_output, index=False)

    def process_elus_files(self, url, colname):
        df_elus = pd.read_csv(url, dtype=str, sep=";")
        df_elus.rename(columns=lambda x: x.replace("’", "'"), inplace=True)

        column_mapping = {
            "Nom de l'élu": "nom_elu",
            "Prénom de l'élu": "prenom_elu",
            "Code sexe": "sexe_elu",
            "Date de naissance": "date_naissance_elu",
            "Libellé de la fonction": "libelle_fonction",
        }

        common_columns = [
            colname,
            "nom_elu",
            "prenom_elu",
            "sexe_elu",
            "date_naissance_elu",
            "libelle_fonction",
        ]

        df_elus = df_elus.rename(columns=column_mapping)[common_columns]
        df_elus = df_elus.rename(
            columns={colname: "colter_code", "libelle_fonction": "fonction_elu"}
        )

        return df_elus
