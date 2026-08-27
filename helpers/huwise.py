from urllib.parse import urlencode

HUWISE_API_PATH = "api/explore/v2.1/catalog/datasets"


def build_export_url(
    domain: str,
    dataset: str,
    columns: list[str] | None = None,
    refine: str | None = None,
    where: str | None = None,
) -> str:
    """
    Construit une URL d'export CSV de l'API Explore v2.1 de Huwise.

    Args:
        domain (str): Domaine hébergeant le jeu de données.
        dataset (str): Identifiant du jeu de données.
        columns (list[str], None): Optionnel. Colonnes à exporter. Les restreindre à celles
            réellement exploitées divise d'autant le volume téléchargé. Par défaut, toutes.
        refine (str, None): Optionnel. Filtre sur une champs intégré : e.g. `familleavis:"radiation"`.
        where (str, None): Optionnel. Clause SQL : e.g. `dateparution >= now(months=-3)`.
    """
    params = {
        "lang": "fr",
        "timezone": "Europe/Paris",
        "use_labels": "true",
        "delimiter": ";",
    }
    if columns:
        params["select"] = ",".join(columns)
    if refine:
        params["refine"] = refine
    if where:
        params["where"] = where

    return (
        f"https://{domain}/{HUWISE_API_PATH}/{dataset}/exports/csv?{urlencode(params)}"
    )
