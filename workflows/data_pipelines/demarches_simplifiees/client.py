from typing import Optional
import logging
import requests
from requests.adapters import HTTPAdapter
from datetime import datetime
from urllib3.util.retry import Retry
from models import (
    DemarcheResponse,
    Dossier,
    PageInfo,
    Demandeur,
    Entreprise,
    Traitement,
    Champ,
)
from queries import (
    ARCHIVE_DOSSIER_MUTATION,
    GET_DEMARCHE_QUERY,
)


class DemarcheSimplifieeClient:
    def __init__(self, api_url: str, auth_token: str):
        self.api_url = api_url
        self.headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json",
        }
        self.session = self._create_persistent_session()

    def _create_persistent_session(self):
        session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def execute_query(self, query: str, variables: dict) -> dict:
        try:
            response = self.session.post(
                self.api_url,
                json={"query": query, "variables": variables},
                headers=self.headers,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"API request failed: {e}")
            raise

    def get_demarche(self, demarche_number: int) -> DemarcheResponse:
        variables = {
            "demarcheNumber": demarche_number,
            "after": None,
            "includeChamps": True,
            "includeTraitements": True,
        }

        all_dossiers = []
        while True:
            data = self.execute_query(GET_DEMARCHE_QUERY, variables)
            demarche_data = data["data"]["demarche"]
            dossiers_data = demarche_data["dossiers"]

            dossiers = self._parse_dossiers(dossiers_data["nodes"])
            all_dossiers.extend(dossiers)

            page_info = self._parse_page_info(dossiers_data["pageInfo"])

            if not page_info.has_next_page:
                break

            variables["after"] = page_info.end_cursor

        return DemarcheResponse(
            id=demarche_data["id"],
            number=int(demarche_data["number"]),
            title=demarche_data["title"],
            state=demarche_data["state"],
            date_creation=datetime.fromisoformat(demarche_data["dateCreation"]),
            dossiers=all_dossiers,
            page_info=page_info,
        )

    def _parse_dossiers(self, dossiers_data: list) -> list[Dossier]:
        return [
            Dossier(
                id=d["id"],
                number=d["number"],
                archived=d["archived"],
                state=d["state"],
                date_derniere_modification=datetime.fromisoformat(
                    d["dateDerniereModification"]
                ),
                date_depot=datetime.fromisoformat(d["dateDepot"]),
                date_passage_en_construction=datetime.fromisoformat(
                    d["datePassageEnConstruction"]
                ),
                date_passage_en_instruction=datetime.fromisoformat(
                    d["datePassageEnInstruction"]
                ),
                date_traitement=(
                    datetime.fromisoformat(d["dateTraitement"])
                    if d["dateTraitement"]
                    else None
                ),
                date_expiration=(
                    datetime.fromisoformat(d["dateExpiration"])
                    if d["dateExpiration"]
                    else None
                ),
                date_suppression_par_usager=(
                    datetime.fromisoformat(d["dateSuppressionParUsager"])
                    if d["dateSuppressionParUsager"]
                    else None
                ),
                demandeur=self._parse_demandeur(d["demandeur"]),
                traitements=self._parse_traitements(d.get("traitements", [])),
                champs=self._parse_champs(d.get("champs", [])),
                usager_email=d["usager"]["email"],
            )
            for d in dossiers_data
        ]

    def _parse_demandeur(self, demandeur_data: Optional[dict]) -> Optional[Demandeur]:
        if not demandeur_data:
            return None

        entreprise_data = demandeur_data.get("entreprise")
        entreprise = None
        if entreprise_data:
            entreprise = Entreprise(
                siren=entreprise_data.get("siren"),
                forme_juridique=entreprise_data.get("formeJuridique"),
                forme_juridique_code=entreprise_data.get("formeJuridiqueCode"),
                nom_commercial=entreprise_data.get("nomCommercial"),
                raison_sociale=entreprise_data.get("raisonSociale"),
                nom=entreprise_data.get("nom"),
                prenom=entreprise_data.get("prenom"),
            )

        return Demandeur(
            siret=demandeur_data.get("siret"),
            siege_social=demandeur_data.get("siegeSocial"),
            entreprise=entreprise,
        )

    def _parse_traitements(self, traitements_data: list) -> list[Traitement]:
        return [
            Traitement(
                state=t["state"],
                email_agent_traitant=t["emailAgentTraitant"],
                date_traitement=(
                    datetime.fromisoformat(t["dateTraitement"])
                    if t["dateTraitement"]
                    else None
                ),
                motivation=t["motivation"],
            )
            for t in traitements_data
        ]

    def _parse_champs(self, champs_data: list) -> list[Champ]:
        return [
            Champ(
                id=c["id"],
                champ_descriptor_id=c["champDescriptorId"],
                type_name=c["__typename"],
                label=c["label"],
                string_value=c["stringValue"],
                updated_at=datetime.fromisoformat(c["updatedAt"]),
                prefilled=c["prefilled"],
            )
            for c in champs_data
        ]

    def _parse_page_info(self, page_info_data: dict) -> PageInfo:
        return PageInfo(
            has_next_page=page_info_data["hasNextPage"],
            has_previous_page=page_info_data["hasPreviousPage"],
            start_cursor=page_info_data["startCursor"],
            end_cursor=page_info_data["endCursor"],
        )

    def archive_dossier(self, dossier_id: str, instructeur_id: str):

        variables = {
            "input": {"dossierId": dossier_id, "instructeurId": instructeur_id}
        }
        return self.execute_query(ARCHIVE_DOSSIER_MUTATION, variables)
