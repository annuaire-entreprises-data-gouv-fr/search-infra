import json
import logging
from pathlib import Path

import pandas as pd
import yaml

from data_pipelines_annuaire.helpers.data_quality import clean_sirent_column
from data_pipelines_annuaire.workflows.data_pipelines.bodacc.utils import (
    extract_sirens_from_listepersonnes,
    fix_mojibake,
    parse_date_bodacc,
    process_discarded_announcements,
)

_INPUT_COLUMNS = [
    "id",
    "listepersonnes",
    "dateparution",
    "typeavis",
    "jugement",
    "parutionavisprecedent",
]

_OUTPUT_COLUMNS = [
    "siren",
    "id_annonce",
    "statut",
    "nature",
    "complement",
    "date",
    "date_publication",
    "cloturee_nature",
]

_RULES_PATH = Path(__file__).parent / "rule.yml"


def _load_procedure_collective_rules() -> list[dict]:
    with open(_RULES_PATH, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data["procedure_collective_rules"]


def _apply_procedure_collective_rules(
    nature: str, complement_jugement: str, rules: list[dict]
) -> str | None:
    """
    Applique les règles dans l'ordre. Retourne le statut de la première règle
    qui matche, ou None si aucune règle ne correspond (avec un warning loggé).
    """
    if not nature:
        return None

    for rule in rules:
        if rule["nature"] != nature:
            continue
        if "complement_contains" in rule:
            if (
                not complement_jugement
                or rule["complement_contains"].lower()
                not in complement_jugement.lower()
            ):
                continue
        return rule.get("statut")

    logging.warning(f"BODACC: nature non traitée dans rule.yml : '{nature}'")
    return None


def _is_cloture(famille: str) -> bool:
    """Vérifie si la famille correspond à une clôture de procédure."""
    FAMILLES_CLOTURE = [
        "jugement de clôture",
    ]
    if pd.isna(famille) or not famille:
        return False
    famille_lower = famille.lower()
    return any(kw in famille_lower for kw in FAMILLES_CLOTURE)


def _parse_jugement_json(jugement_str: str) -> dict:
    """Parse le champ json jugement et en extraire famille, nature, complementJugement et date."""
    if pd.isna(jugement_str) or not jugement_str:
        return {"famille": "", "nature": "", "complementJugement": "", "date": ""}
    try:
        data = json.loads(jugement_str)
        return {
            "famille": fix_mojibake(data.get("famille", "")),
            "nature": fix_mojibake(data.get("nature", "")),
            "complementJugement": fix_mojibake(data.get("complementJugement", "")),
            "date": parse_date_bodacc(data.get("date", "")),
        }
    except json.JSONDecodeError:
        return {"famille": "", "nature": "", "complementJugement": "", "date": ""}


def _process_procedure_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    """Traiter un chunk de données procédures collectives."""
    # Un avis de procédure collective peut concerner qu'une ou plusieurs "personnes"
    chunk = extract_sirens_from_listepersonnes(chunk)

    # Nettoyer et valider les SIREN
    chunk = clean_sirent_column(
        chunk,
        column_type="siren",
        column_name="siren",
        add_leading_zeros=True,
        max_removal_percentage=0.1,
    )

    if chunk.empty:
        return chunk

    # Parser le JSON jugement (famille, nature, date)
    chunk["jugement_parsed"] = chunk["jugement"].apply(_parse_jugement_json)
    chunk["procedure_collective_famille"] = chunk["jugement_parsed"].apply(
        lambda x: x.get("famille", "") if x else ""
    )

    # Ces familles ne représentent pas des procédures collectives à proprement parler :
    # - "Avis de dépôt" : simple formalité de dépôt de créances
    # - "Extrait de jugement" : cas marginaux mal catégorisés
    # - "Loi de 1967" : régime juridique obsolète
    familles_exclues = ["Avis de dépôt", "Extrait de jugement", "Loi de 1967"]
    chunk = chunk[~chunk["procedure_collective_famille"].isin(familles_exclues)]

    if chunk.empty:
        return chunk

    chunk["nature"] = chunk["jugement_parsed"].apply(
        lambda x: x.get("nature", "") if x else ""
    )
    chunk["complement"] = chunk["jugement_parsed"].apply(
        lambda x: x.get("complementJugement", "") if x else ""
    )
    chunk["date"] = chunk["jugement_parsed"].apply(
        lambda x: x.get("date", "") if x else ""
    )

    # Garder uniquement les colonnes nécessaires
    chunk["date_publication"] = chunk["dateparution"]
    chunk["id_annonce"] = chunk["id"]
    return chunk[
        [
            "siren",
            "id_annonce",
            "procedure_collective_famille",
            "nature",
            "complement",
            "date",
            "date_publication",
        ]
    ]


def process_procedures_collectives(raw_file_path: str, chunk_size: int) -> pd.DataFrame:
    logging.info("Processing procédures collectives...")
    chunks_processed = []

    # Charger le fichier complet pour gérer les annulations
    df_full = pd.read_csv(
        raw_file_path,
        dtype=str,
        sep=";",
        usecols=_INPUT_COLUMNS,
    )
    logging.info(f"Loaded {len(df_full)} procedure rows")

    # Filtrer les annulations et rétractations
    df_full = process_discarded_announcements(df_full)
    logging.info(f"After filtering cancellations: {len(df_full)} rows")

    # Traiter par chunks pour la mémoire
    for i in range(0, len(df_full), chunk_size):
        chunk = df_full.iloc[i : i + chunk_size]
        processed = _process_procedure_chunk(chunk)
        if not processed.empty:
            chunks_processed.append(processed)

    if not chunks_processed:
        logging.warning("No procedures collectives found")
        return pd.DataFrame(columns=_OUTPUT_COLUMNS)

    # Concaténer tous les chunks
    df = pd.concat(chunks_processed, ignore_index=True)

    # Appliquer les règles de classification
    rules = _load_procedure_collective_rules()
    df["statut"] = df.apply(
        lambda row: _apply_procedure_collective_rules(
            row["nature"],
            row["complement"],
            rules,
        ),
        axis=1,
    )

    # Environ 5 dates de procédures collectives avec des dates absurdes comme 0009-12-02 ou 5025-05-27
    # Ces "erreurs" sont ignorées lors des conversions en datetime
    df["date"] = pd.to_datetime(df["date"], errors="coerce", format="%Y-%m-%d")
    df["date_publication"] = pd.to_datetime(
        df["date_publication"],
        errors="coerce",
        format="%Y-%m-%d",
    )

    # Dédupliquer par SIREN en gardant la procédure la plus récente
    df = df.sort_values(
        [
            "date",
            "date_publication",
        ],
        ascending=[False, False],
    )
    duplicated_mask = df.duplicated(subset=["siren"], keep="first")
    n_duplicates = duplicated_mask.sum()
    if n_duplicates > 0:
        sample_sirens = df.loc[duplicated_mask, "siren"].head(5).tolist()
        logging.info(
            f"Procedures: {n_duplicates} duplicate Siren, sample:\n{sample_sirens}"
        )
    df = df.drop_duplicates(subset=["siren"], keep="first")

    # Si le dernier jugement est une clôture, la procédure n'est plus active.
    df["is_cloture"] = df["procedure_collective_famille"].apply(_is_cloture)

    # Au-delà de 10 ans sans nouveau jugement, on considère la procédure
    # comme expirée. C'est une règle de précaution car les procédures
    # ne sont pss censées durer aussi longtemps.
    ten_years_ago = pd.Timestamp.now() - pd.DateOffset(years=10)
    df["is_expired"] = df["date"] < ten_years_ago

    # Créer les colonnes finales
    df["cloturee_nature"] = df.apply(
        lambda row: row["nature"] if row["is_cloture"] else "",
        axis=1,
    )
    df["nature"] = df.apply(
        lambda row: "" if row["is_cloture"] or row["is_expired"] else row["nature"],
        axis=1,
    )

    # Effacer le statut pour les procédures clôturées ou expirées
    df["statut"] = df.apply(
        lambda row: (
            "" if row["is_cloture"] or row["is_expired"] else row["statut"] or ""
        ),
        axis=1,
    )

    logging.info(
        f"Procedures: {len(df)} unique SIRENs, "
        f"{df['is_cloture'].sum()} clôturées, "
        f"{(~df['is_cloture'] & df['is_expired']).sum()} expirées (>10 ans), "
        f"{(~df['is_cloture'] & ~df['is_expired']).sum()} en cours"
    )

    return df[_OUTPUT_COLUMNS]
