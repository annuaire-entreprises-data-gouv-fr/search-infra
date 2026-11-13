from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from airflow.sdk import Variable

from data_pipelines_annuaire.helpers.api_client import ApiClient


class _GraphQLClient(ApiClient):
    def __init__(self) -> None:
        super().__init__(
            base_url="https://demarche.numerique.gouv.fr",
            headers={
                "Authorization": f"Bearer {Variable.get('DEMARCHE_NUMERIQUE_API_TOKEN')}"
            },
        )

    def query(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        """Run a GraphQL query or mutation and return its `data`."""
        response_json = self.post(
            endpoint="/api/v2/graphql",
            json={"query": query, "variables": variables},
        ).json()
        # GraphQL errors also come back with a 200 status code.
        if response_json.get("errors"):
            raise RuntimeError(
                f"Démarche Numérique API errors: {response_json['errors']}"
            )
        return response_json["data"]


@dataclass(frozen=True)
class Champ:
    """A field of a dossier as filled in by the user.

    The API returns a different JSON structure for each field type (text, date,
    SIRET, etc.) with nulls and nested objects. A Champ always has the same
    attributes so that the business code can read it without knowing the API.

    Attributes:
        label: label of the field in the form as written by its creator.
        value: the raw answer as text. Filled in for every field type. Empty
            string when the field was left blank.
        date: the YYYY-MM-DD format. Only for a date field. None otherwise.
        siret: The SIRET of the établissement if validated by Démarche Numérique.
            None otherwise.

    Prioritise date and siret when they are set and fall back to parsing
    value if those are empty.

    Note that many other types do exist, but only the ones currently in use in
    our forms have been implemented.
    See: https://demarche.numerique.gouv.fr/graphql/schema/types/Champ#implementations
    """

    label: str
    value: str
    date: str | None = None
    siret: str | None = None

    @classmethod
    def from_api(cls, node: dict[str, Any]) -> Champ:
        """
        Build a champ from a JSON element of champs coming from the API response.

        `stringValue` and `label` exist for every field type.
        `date`, `etablissement`, etc. are only returned for the types specifically request in the query:
            * `... on DateChamp`
            * `... on SiretChamp`
        For example `etablissement` may be null if the SIRET could not be validated by Démarche Numérique.
        """
        return cls(
            label=node["label"],
            value=node.get("stringValue") or "",
            date=node.get("date"),
            siret=(node.get("etablissement") or {}).get("siret"),
        )


class Dossier:
    """A request submitted by a user on a démarche.

    Notes on a few Dossier fields:
    * `id` is a private unique GraphQL ID
    * `number` is the public ID shown in the Démarche Numérique interface and URLs
    * `champs` is a JSON containing all the custom fields setup by the creator of the démarche
    * `dateTraitement` is the date of the latest decision (accepted, refused, etc.)
    * `traitements` is the history of the state changes, with the email of the
      instructeur behind each one (null when made automatically)

    Payload example:
    ```json
    {
        "data": {
        "dossier": {
            "id": "ABC",
            "number": 123,
            "state": "en_construction",
            "archived": false,
            "dateDepot": "2026-11-12T18:35:52+01:00",
            "dateTraitement": null,
            "demandeur": {
                "nom": "Hier",
                "prenom": "Demain",
                "dateDeNaissance": null
            },
            "champs": [
                {
                    "label": "Numéro SIRET",
                    "stringValue": "13002526500013",
                    "etablissement": {
                    "siret": "13002526500013"
                    }
                },
                {
                    "label": "Date de naissance",
                    "stringValue": "12 novembre 1994",
                    "date": "1994-11-12"
                },
                {
                    "label": "Votre question hier ou demain",
                    "stringValue": "Pourquoi pas aujourd'hui ?"
                }
            ]
        }
    }
    ```

    Example:
        > for dossier in Demarche(123, instructeur_id).accepted_dossiers():
        >     siren = dossier.champ("SIREN")
        >     ...
        >     dossier.archive()
    """

    def __init__(self, demarche: Demarche, node: dict[str, Any]) -> None:
        self.demarche = demarche
        self.id: str = node["id"]
        self.number: int = node["number"]
        self.archived: bool = node.get("archived", False)
        self.date_depot: str = (node.get("dateDepot") or "")[:10]
        self.date_traitement: str = (node.get("dateTraitement") or "")[:10]
        traitements = sorted(
            node.get("traitements") or [],
            key=lambda t: datetime.fromisoformat(t["dateTraitement"]),
        )
        # Email of the instructeur who made the latest decision, empty if it
        # was made automatically.
        self.email_instructeur: str = (
            traitements[-1].get("emailAgentTraitant") or "" if traitements else ""
        )
        demandeur = node.get("demandeur") or {}
        self.demandeur_nom: str = demandeur.get("nom") or ""
        self.demandeur_prenom: str = demandeur.get("prenom") or ""
        self.champs = [Champ.from_api(champ) for champ in node.get("champs", [])]

    @property
    def url(self) -> str:
        return self.demarche.dossier_url(self.number)

    def champ(self, label: str) -> Champ | None:
        """
        Return the field with this label or None if the form has none.
        If several fields share a label, the first one is returned.
        """
        label = label.strip().lower()
        return next((c for c in self.champs if c.label.strip().lower() == label), None)

    def archive(self) -> None:
        """Archive a dossier."""
        archive_mutation = """
            mutation archiveDossier($input: DossierArchiverInput!) {
              dossierArchiver(input: $input) {
                errors {
                  message
                }
              }
            }
        """
        errors = self.demarche._client.query(
            archive_mutation,
            {
                "input": {
                    "dossierId": self.id,
                    "instructeurId": self.demarche.instructeur_id,
                }
            },
        )["dossierArchiver"]["errors"]
        if errors:
            raise RuntimeError("; ".join(e["message"] for e in errors))
        self.archived = True


class Demarche:
    """
    A form of Démarche Numérique.

    Args:
        number: number of the démarche, as in its URLs.
        instructeur_id: GraphQL ID of the instructeur account the actions on
            its dossiers (archiving…) are recorded under. It must be an
            instructeur of the démarche.

    Example:
        > demarche = Demarche(123, instructeur_id)
        > for dossier in demarche.accepted_dossiers():
        >     ...
    """

    def __init__(self, number: int, instructeur_id: str) -> None:
        self.number = number
        self.instructeur_id = instructeur_id
        self._client: _GraphQLClient = _GraphQLClient()

    def dossier_url(self, dossier_number: int) -> str:
        return f"{self._client.base_url}/procedures/{self.number}/dossiers/{dossier_number}"

    def accepted_dossiers(self) -> Iterator[Dossier]:
        """Iterate over the accepted dossiers not archived yet."""
        after = None
        accepted_dossiers_query = """
            query getDossiers($demarcheNumber: Int!, $after: String) {
              demarche(number: $demarcheNumber) {
                dossiers(state: accepte, archived: false, first: 100, after: $after) {
                  pageInfo {
                    hasNextPage
                    endCursor
                  }
                  nodes {
                    id
                    number
                    archived
                    dateDepot
                    dateTraitement
                    traitements {
                      dateTraitement
                      emailAgentTraitant
                    }
                    demandeur {
                      ... on PersonnePhysique {
                        nom
                        prenom
                      }
                    }
                    champs {
                      label
                      stringValue
                      ... on DateChamp {
                        date
                      }
                      ... on SiretChamp {
                        etablissement {
                          siret
                        }
                      }
                    }
                  }
                }
              }
            }
        """
        while True:
            dossiers = self._client.query(
                accepted_dossiers_query,
                {"demarcheNumber": self.number, "after": after},
            )["demarche"]["dossiers"]
            yield from (Dossier(self, node) for node in dossiers["nodes"])
            if not dossiers["pageInfo"]["hasNextPage"]:
                return
            after = dossiers["pageInfo"]["endCursor"]
