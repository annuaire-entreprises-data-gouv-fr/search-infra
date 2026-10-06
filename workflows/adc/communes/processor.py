import hashlib
import json
import logging
import os
import re
from collections import defaultdict
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from itertools import groupby
from operator import itemgetter
from pathlib import Path

import pandas as pd
from airflow.sdk import task
from boto3.s3.transfer import TransferConfig

from data_pipelines_annuaire.config import SIRENE_OBJECT_STORAGE_DATA_PATH
from data_pipelines_annuaire.helpers import (
    DataProcessor,
    Notification,
    ObjectStorageClient,
    SqliteClient,
)
from data_pipelines_annuaire.helpers.datagouv import (
    get_dataset_or_resource_metadata,
    get_resource,
    get_resource_metadata,
)
from data_pipelines_annuaire.workflows.adc.communes.config import (
    ADC_COMMUNES_OBJECT_STORAGE_PATH,
    COG_COMER_FILE,
    COG_COMER_NATURES,
    COG_COMER_RESOURCE_TITLE,
    COG_DATASET_ID,
    COG_FILE,
    COG_RESOURCE_TITLE,
    DONNEES_SOURCES,
    EFFECTIFS_FILE,
    EFFECTIFS_RESOURCE_ID,
    FLUX_YEARS,
    JSON_OUTPUT_DIR,
    MIN_EXPECTED_COMMUNES,
    MIN_EXPECTED_ETABLISSEMENTS,
    SIRENE_DATABASE_LOCATION,
    SOURCE_EFFECTIFS,
    SOURCE_SIRENE,
    UPLOAD_MULTIPART_THRESHOLD,
    UPLOAD_THREADS,
    WORK_DATABASE_LOCATION,
)
from data_pipelines_annuaire.workflows.adc.communes.queries import (
    EFFECTIFS_QUERY,
    ETABLISSEMENTS_QUERY,
    FLUX_QUERY,
)

logger = logging.getLogger(__name__)

EFFECTIFS_COLUMN_PATTERN = re.compile(r"^Effectifs salariés (\d{4})$")
GRAND_SECTEUR_PREFIX_PATTERN = re.compile(r"GS\d+\s*")


def _download_resource(resource_id: str, destination: str) -> None:
    path = Path(destination)
    get_resource(resource_id, {"dest_path": f"{path.parent}/", "dest_name": path.name})


@task
def get_latest_sirene_database() -> str:
    """Retourne la date de la base SIRENE : YYYY-MM-DD."""
    return ObjectStorageClient().get_latest_database(
        SIRENE_OBJECT_STORAGE_DATA_PATH,
        SIRENE_DATABASE_LOCATION,
    )[:10]


def _download_latest_cog_resource(
    resources: list[dict], title: str, destination: str
) -> None:
    """Le COG publie une ressource par millésime : on prend la plus récente."""
    resource = max(
        (r for r in resources if r["format"] == "csv" and title in r["title"]),
        key=itemgetter("last_modified"),
    )
    logger.info(f"COG : {resource['title']} ({resource['id']})")
    _download_resource(resource["id"], destination)


@task
def load_communes() -> None:
    """
    Charge les communes, arrondissements municipaux et communes des collectivités
    d'outre-mer du dernier millésime du COG.
    """
    resources = get_dataset_or_resource_metadata(dataset_id=COG_DATASET_ID)["resources"]
    _download_latest_cog_resource(resources, COG_RESOURCE_TITLE, COG_FILE)
    _download_latest_cog_resource(resources, COG_COMER_RESOURCE_TITLE, COG_COMER_FILE)

    df = pd.read_csv(COG_FILE, dtype=str, usecols=["TYPECOM", "COM", "COMPARENT"])
    df = df[df["TYPECOM"].isin(["COM", "ARM"])]
    df["parent"] = df["COMPARENT"].where(df["TYPECOM"] == "ARM")
    df = df.rename(columns={"COM": "code"})[["code", "parent"]]

    df_comer = pd.read_csv(
        COG_COMER_FILE, dtype=str, usecols=["COM_COMER", "NATURE_ZONAGE"]
    )
    df_comer = df_comer[df_comer["NATURE_ZONAGE"].isin(COG_COMER_NATURES)]
    df_comer = df_comer.rename(columns={"COM_COMER": "code"})[["code"]]

    df = pd.concat([df, df_comer]).drop_duplicates(subset="code")

    with SqliteClient(WORK_DATABASE_LOCATION) as sqlite_client:
        df.to_sql("commune", sqlite_client.db_conn, if_exists="replace", index=False)
    logger.info(f"{len(df)} communes et arrondissements municipaux chargés.")


@task
def load_effectifs() -> str:
    """
    Agrège les effectifs salariés par commune, année et grand secteur d'activité.
    Retourne la date de mise à jour de la ressource data.gouv (YYYY-MM-DD).
    """
    _download_resource(EFFECTIFS_RESOURCE_ID, EFFECTIFS_FILE)

    header = pd.read_csv(EFFECTIFS_FILE, sep=";", encoding="utf-8-sig", nrows=0)
    year_columns = {
        column: match.group(1)
        for column in header.columns
        if (match := EFFECTIFS_COLUMN_PATTERN.match(column))
    }
    keys = ["Code commune", "Grand secteur d'activité"]

    chunks = []
    for chunk in pd.read_csv(
        EFFECTIFS_FILE,
        sep=";",
        encoding="utf-8-sig",
        dtype=str,
        usecols=keys + list(year_columns),
        chunksize=200_000,
    ):
        chunk[list(year_columns)] = chunk[list(year_columns)].apply(
            pd.to_numeric, errors="coerce"
        )
        chunks.append(chunk.groupby(keys).sum(min_count=1))

    df = (
        pd.concat(chunks)
        .groupby(level=keys)
        .sum()
        .rename(columns=year_columns)
        .reset_index()
        .melt(id_vars=keys, var_name="annee", value_name="effectif")
    )
    df = df[df["effectif"] > 0]
    df = df.rename(
        columns={
            "Code commune": "code_commune",
            "Grand secteur d'activité": "grand_secteur_activite",
        }
    )
    df["grand_secteur_activite"] = df["grand_secteur_activite"].str.replace(
        GRAND_SECTEUR_PREFIX_PATTERN, "", regex=True
    )
    df["effectif"] = df["effectif"].astype(int)

    with SqliteClient(WORK_DATABASE_LOCATION) as sqlite_client:
        df.to_sql("effectifs", sqlite_client.db_conn, if_exists="replace", index=False)
    logger.info(f"{len(df)} lignes d'effectifs chargées.")
    return get_resource_metadata(EFFECTIFS_RESOURCE_ID)["resource"]["last_modified"][
        :10
    ]


@dataclass
class CommunePayload:
    effectifs: defaultdict[tuple[str, str], int] = field(
        default_factory=lambda: defaultdict(int)
    )
    etablissements: list[dict] = field(default_factory=list)
    flux: defaultdict[str, list[int]] = field(
        default_factory=lambda: defaultdict(lambda: [0, 0])
    )

    def merge(self, other: "CommunePayload") -> None:
        for key, effectif in other.effectifs.items():
            self.effectifs[key] += effectif
        self.etablissements.extend(other.etablissements)
        for mois, (ouvertures, fermetures) in other.flux.items():
            self.flux[mois][0] += ouvertures
            self.flux[mois][1] += fermetures

    def to_dict(self) -> dict:
        effectifs_par_annee: dict[str, list[dict]] = {}
        for (annee, secteur), effectif in sorted(
            self.effectifs.items(), key=lambda item: (item[0][0], item[1]), reverse=True
        ):
            effectifs_par_annee.setdefault(annee, []).append(
                {"effectif": effectif, "grand_secteur_activite": secteur}
            )
        return {
            "effectif_salaries": effectifs_par_annee,
            "etablissements_sirene": self.etablissements,
            "flux_ouverture_etablissements": [
                {"mois": mois, "ouvertures": ouvertures, "fermetures": fermetures}
                for mois, (ouvertures, fermetures) in sorted(self.flux.items())
            ],
        }


class SortedGroups:
    """Lit un flux de lignes trié par code commune, groupe par groupe, à la demande."""

    def __init__(self, rows: Iterator[tuple]):
        self._groups = groupby(rows, key=itemgetter(0))
        self._current = next(self._groups, None)
        self.skipped_codes: list[str] = []
        self.skipped_rows = 0

    def _skip_current(self) -> None:
        assert self._current is not None
        self.skipped_codes.append(self._current[0])
        self.skipped_rows += sum(1 for _ in self._current[1])
        self._current = next(self._groups, None)

    def pop(self, code: str) -> list[tuple]:
        while self._current is not None and self._current[0] < code:
            self._skip_current()
        if self._current is None or self._current[0] != code:
            return []
        rows = list(self._current[1])
        self._current = next(self._groups, None)
        return rows

    def skip_remaining(self) -> None:
        """Comptabilise les codes situés après le dernier code demandé."""
        while self._current is not None:
            self._skip_current()


def _to_coordinate(value: str | None) -> float | None:
    try:
        return round(float(value), 6)
    except (TypeError, ValueError):
        return None


def _write_json_files(
    code: str, payload: CommunePayload, dates_mise_a_jour: dict[str, str]
) -> None:
    commune_dir = f"{JSON_OUTPUT_DIR}{code}/"
    os.makedirs(commune_dir, exist_ok=True)
    for nom, donnees in payload.to_dict().items():
        source = DONNEES_SOURCES[nom]
        envelope = {
            "date_mise_a_jour": dates_mise_a_jour[source],
            "source": source,
            "donnees": donnees,
        }
        with open(f"{commune_dir}{nom}.json", "w", encoding="utf-8") as f:
            json.dump(envelope, f, ensure_ascii=False, separators=(",", ":"))


def _list_local_files() -> list[str]:
    """Chemins des JSON générés, relatifs à JSON_OUTPUT_DIR : {code}/{nom}.json."""
    return [
        f"{code}/{file_name}"
        for code in os.listdir(JSON_OUTPUT_DIR)
        for file_name in os.listdir(f"{JSON_OUTPUT_DIR}{code}")
    ]


def _list_remote_files(object_storage: ObjectStorageClient) -> dict[str, str]:
    """{chemin relatif au préfixe: ETag} des objets présents sous le préfixe."""
    paginator = object_storage.client.get_paginator("list_objects_v2")
    return {
        obj["Key"].removeprefix(ADC_COMMUNES_OBJECT_STORAGE_PATH): obj["ETag"].strip(
            '"'
        )
        for page in paginator.paginate(
            Bucket=object_storage.bucket, Prefix=ADC_COMMUNES_OBJECT_STORAGE_PATH
        )
        for obj in page.get("Contents", [])
    }


@task
def generate_json_files(sirene_date: str, effectifs_date: str) -> None:
    """
    Écrit, pour chaque commune et arrondissement municipal du COG, un JSON par type
    de donnée, y compris sans données.

    Les trois sources sont lues triées par code commune en même temps que la liste
    triée des communes : une seule commune est en mémoire à la fois. Seules Paris,
    Lyon et Marseille, dont SIRENE et les effectifs ne connaissent que les
    arrondissements, sont gardées jusqu'à la fin pour y cumuler leurs arrondissements.
    """
    os.makedirs(JSON_OUTPUT_DIR, exist_ok=True)
    dates_mise_a_jour = {
        SOURCE_SIRENE: sirene_date,
        SOURCE_EFFECTIFS: effectifs_date,
    }

    today = datetime.now(tz=UTC).date()
    flux_params = {
        "date_debut": today.replace(year=today.year - FLUX_YEARS, day=1).isoformat(),
        "date_fin": today.isoformat(),
    }

    with (
        SqliteClient(WORK_DATABASE_LOCATION) as work_db,
        SqliteClient(SIRENE_DATABASE_LOCATION) as sirene_db,
    ):
        communes = sorted(work_db.execute("SELECT code, parent FROM commune"))
        effectifs = SortedGroups(work_db.db_conn.execute(EFFECTIFS_QUERY))
        etablissements = SortedGroups(sirene_db.db_conn.execute(ETABLISSEMENTS_QUERY))
        flux = SortedGroups(sirene_db.db_conn.execute(FLUX_QUERY, flux_params))

        parents = {parent for _, parent in communes if parent}
        deferred: dict[str, CommunePayload] = {}
        etablissements_count = 0

        for code, parent in communes:
            payload = CommunePayload()
            for _, annee, secteur, effectif in effectifs.pop(code):
                payload.effectifs[(annee, secteur)] += effectif
            for _, nom, siret, latitude, longitude in etablissements.pop(code):
                payload.etablissements.append(
                    {
                        "nom": nom,
                        "siret": siret,
                        "lat": _to_coordinate(latitude),
                        "lon": _to_coordinate(longitude),
                    }
                )
            for _, mois, ouvertures, fermetures in flux.pop(code):
                payload.flux[mois] = [ouvertures, fermetures]
            etablissements_count += len(payload.etablissements)

            if code in parents:
                deferred.setdefault(code, CommunePayload()).merge(payload)
                continue
            _write_json_files(code, payload, dates_mise_a_jour)
            if parent:
                deferred.setdefault(parent, CommunePayload()).merge(payload)

        for code, payload in deferred.items():
            _write_json_files(code, payload, dates_mise_a_jour)

        for name, stream in [
            ("effectifs", effectifs),
            ("établissements", etablissements),
            ("flux", flux),
        ]:
            stream.skip_remaining()
            logger.info(
                f"{name} : {stream.skipped_rows} lignes ignorées sur "
                f"{len(stream.skipped_codes)} codes commune absents du COG "
                f"{stream.skipped_codes[:50]}"
            )
    if len(communes) < MIN_EXPECTED_COMMUNES:
        raise ValueError(f"Seulement {len(communes)} communes dans le COG.")
    if etablissements_count < MIN_EXPECTED_ETABLISSEMENTS:
        raise ValueError(f"Seulement {etablissements_count} établissements.")

    DataProcessor.push_message(
        Notification.notification_xcom_key,
        description=f"{len(communes)} communes générées "
        f"({etablissements_count} établissements).",
    )


@task
def upload_json_files() -> None:
    """Envoie les JSON absents ou modifiés, détectés en comparant leur MD5 à l'ETag distant."""
    object_storage = ObjectStorageClient()
    remote_files = _list_remote_files(object_storage)
    local_files = _list_local_files()
    transfer_config = TransferConfig(multipart_threshold=UPLOAD_MULTIPART_THRESHOLD)

    def upload_if_changed(path: str) -> bool:
        local_path = f"{JSON_OUTPUT_DIR}{path}"
        with open(local_path, "rb") as f:
            if hashlib.md5(f.read()).hexdigest() == remote_files.get(path):
                return False
        object_storage.client.upload_file(
            local_path,
            object_storage.bucket,
            f"{ADC_COMMUNES_OBJECT_STORAGE_PATH}{path}",
            ExtraArgs={
                "ACL": "public-read",
                "ContentType": "application/json; charset=utf-8",
            },
            Config=transfer_config,
        )
        return True

    with ThreadPoolExecutor(max_workers=UPLOAD_THREADS) as executor:
        # sum() consomme les résultats et propage la première exception d'upload
        uploaded_count = sum(executor.map(upload_if_changed, local_files))

    DataProcessor.push_message(
        Notification.notification_xcom_key,
        description=f"{uploaded_count} fichiers modifiés envoyés sur {len(local_files)}.",
    )


@task
def check_and_delete_stale_json_files() -> None:
    """
    Vérifie que tous les JSON générés sont sur l'Object Storage, puis supprime les
    autres objets du préfixe (communes sorties du COG, anciens types de donnée).
    """
    commune_count = len(os.listdir(JSON_OUTPUT_DIR))
    if commune_count < MIN_EXPECTED_COMMUNES:
        raise ValueError(f"Seulement {commune_count} communes générées localement.")

    object_storage = ObjectStorageClient()
    local_files = set(_list_local_files())
    remote_files = set(_list_remote_files(object_storage))

    missing_files = local_files - remote_files
    if missing_files:
        raise ValueError(
            f"{len(missing_files)} fichiers absents après l'upload : "
            f"{sorted(missing_files)[:20]}"
        )

    stale_keys = [
        f"{ADC_COMMUNES_OBJECT_STORAGE_PATH}{path}"
        for path in sorted(remote_files - local_files)
    ]
    for i in range(0, len(stale_keys), 1000):
        object_storage.client.delete_objects(
            Bucket=object_storage.bucket,
            Delete={"Objects": [{"Key": key} for key in stale_keys[i : i + 1000]]},
        )
    logger.info(f"{len(stale_keys)} fichiers obsolètes supprimés : {stale_keys[:100]}")
