import gzip
import json
import logging
import os
import re
import shutil
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from airflow.sdk import get_current_context, task

from data_pipelines_annuaire.config import (
    OBJECT_STORAGE_ENV_PATH,
    RNE_DEFAULT_START_DATE,
    RNE_FLUX_DATADIR,
    RNE_OBJECT_STORAGE_FLUX_DATA_PATH,
)
from data_pipelines_annuaire.helpers import Notification, ObjectStorageClient
from data_pipelines_annuaire.helpers.utils import get_last_lines
from data_pipelines_annuaire.workflows.data_pipelines.rne.flux.rne_api import (
    ApiRNEClient,
)

logger = logging.getLogger(__name__)

FLUX_FILE_PATTERN = re.compile(r"rne_flux_(\d{4}-\d{2}-\d{2})\.json\.gz$")


@dataclass(order=True)
class RneFluxFile:
    """
    An RNE Flux File is a json file containing all the updates done on the RNE on
    companies for the relevant day. It is built locally and saved on the object storage.
    Those files are not json compliant.
    Files are ordered by date.
    """

    flux_date: str
    path: str

    @classmethod
    def parse(cls, file_path: str) -> "RneFluxFile | None":
        """Build a RneFluxFile from its path or return None if the path is not a flux file."""
        match = FLUX_FILE_PATTERN.search(file_path)
        if not match:
            return None
        return cls(match.group(1), file_path)

    @classmethod
    def list_on_object_storage(cls) -> list["RneFluxFile"]:
        """List the flux files hosted on the object storage. The output is sorted."""
        files_on_object_storage = ObjectStorageClient().get_files_from_prefix(
            prefix=RNE_OBJECT_STORAGE_FLUX_DATA_PATH,
        )
        return sorted(
            flux_file
            for flux_file in map(cls.parse, files_on_object_storage)
            if flux_file
        )

    @classmethod
    def last_on_object_storage(cls) -> str | None:
        """Return the date of the last file hosted on the object storage or None if none is saved yet."""
        files = cls.list_on_object_storage()
        if not files:
            return None

        last_date = files[-1].flux_date
        logger.info(f"Last date found: {last_date}")
        return last_date

    @classmethod
    def get_from_object_storage(cls, flux_date: str) -> "RneFluxFile | None":
        """Return the file for a date from the object storage or None if it has none."""
        saved = [
            file for file in cls.list_on_object_storage() if file.flux_date == flux_date
        ]
        return saved[-1] if saved else None

    @classmethod
    def build_name(cls, flux_date: str) -> str:
        """Name of the non-compressed JSON file of a date."""
        return f"rne_flux_{flux_date}.json"

    @property
    def name(self) -> str:
        return os.path.basename(self.path)

    def download(self) -> str:
        """Download and unzip the file and then return the path of the local JSON file."""
        local_path = f"{RNE_FLUX_DATADIR}/{self.name.removesuffix('.gz')}"

        ObjectStorageClient().get_object_object_storage(
            f"{OBJECT_STORAGE_ENV_PATH}{RNE_OBJECT_STORAGE_FLUX_DATA_PATH}",
            self.name,
            f"{local_path}.gz",
        )
        logger.info(f"Downloaded zip file: {self.name}")

        with (
            gzip.open(f"{local_path}.gz", "rb") as f_in,
            open(local_path, "wb") as f_out,
        ):
            shutil.copyfileobj(f_in, f_out)

        os.remove(f"{local_path}.gz")
        return local_path

    @classmethod
    def upload(cls, json_file_path: str, flux_date: str) -> None:
        """Compress and upload the local JSON file of a date, then free it from local storage."""
        dest_name = f"{cls.build_name(flux_date)}.gz"
        ObjectStorageClient().upload_compressed_file(
            source_file_path=json_file_path,
            object_storage_path=RNE_OBJECT_STORAGE_FLUX_DATA_PATH,
            dest_name=dest_name,
        )
        logger.info(f"Sent file to the object storage: {dest_name}")

        os.remove(json_file_path)
        logger.info(f"Deleted local file: {json_file_path}")


def read_siren(line: bytes) -> str | None:
    """Return the SIREN of a flux line or None if the line is corrupted or incomplete."""
    try:
        return (json.loads(line).get("company") or {}).get("siren")
    except ValueError:
        return None


def get_last_siren(local_path: str) -> str | None:
    """
    Get the last SIREN from a flux file.
    If the last two lines contain a corrupted JSON or a missing SIREN return none.
    """
    lines = get_last_lines(local_path, 2)
    if not lines:
        return None

    last_siren = read_siren(lines[-1])
    if last_siren is not None:
        return last_siren

    faulty_line = lines.pop()
    logger.warning(
        f"No SIREN in the last line of {local_path}, trying the previous one."
    )
    last_siren = read_siren(lines[-1]) if lines else None
    if last_siren is None:
        logger.warning(
            f"No SIREN in the previous line either. File {local_path} is dead."
        )
        return None

    os.truncate(local_path, os.path.getsize(local_path) - len(faulty_line))
    logger.info(f"Dropped the faulty last line of {local_path}.")
    return last_siren


def get_resume_point(flux_date: str) -> str | None:
    """
    Find where to resume the enrichment of a flux file that was only partially processed.
    If no SIREN is found, return none.
    The partial file is kept locally so the new records can be appended to it later on.
    """
    file = RneFluxFile.get_from_object_storage(flux_date)
    if file is None:
        logger.info(f"No file found for {flux_date}, starting from scratch.")
        return None

    local_path = file.download()
    try:
        last_siren = get_last_siren(local_path)
    except Exception as e:
        # The file is corrupted so no cursor to resume from
        logger.warning(f"Unable to find a resume point in {file.path}: {e}")
        last_siren = None

    if last_siren is None:
        logger.warning(
            f"No SIREN to resume from for {flux_date}: deleting the local file."
        )
        os.remove(local_path)
    return last_siren


def compute_start_date():
    last_json_date = RneFluxFile.last_on_object_storage()

    if last_json_date:
        last_date_obj = date.fromisoformat(last_json_date)
        start_date = last_date_obj.strftime("%Y-%m-%d")
        logger.info(f"Start date: {start_date}")
    else:
        start_date = RNE_DEFAULT_START_DATE

    return start_date


def get_and_save_daily_flux_rne(
    start_date: str,
    end_date: str,
    resume_from_saved_file: bool,
) -> None:
    """
    Fetches daily flux data from RNE API, stores it in a JSON file, and sends the
    file to the object storage once the whole day has been processed.

    Args:
        start_date (str): The start date for data retrieval in the format 'YYYY-MM-DD'.
        end_date (str): The end date for data retrieval in the format 'YYYY-MM-DD'.
        resume_from_saved_file (bool): If true, resume the processing of the period.
    """
    json_file_name = RneFluxFile.build_name(start_date)
    json_file_path = f"{RNE_FLUX_DATADIR}/{json_file_name}"
    os.makedirs(RNE_FLUX_DATADIR, exist_ok=True)
    if resume_from_saved_file:
        logger.info(f"Try resuming the processing of the day {start_date}..")
        last_siren = get_resume_point(start_date)
        if last_siren is None:
            logger.info(
                f"No SIREN to resume from found. Rebuilding the day {start_date} from scratch."
            )
        else:
            logger.info(f"Last siren found for {start_date}: {last_siren}")
    else:
        last_siren = None  # Initialize last_siren
    page_data = True

    rne_client = ApiRNEClient()

    fetched_records = 0
    with open(json_file_path, "a" if last_siren else "w") as json_file:
        logger.info(f"Opening file: {json_file_path}")
        try:
            # When the API answers with an empty page it means the whole
            # day has been fetched
            while page_data:
                page_data, last_siren = rne_client.make_api_request(
                    start_date, end_date, last_siren
                )
                for company in page_data or []:
                    json.dump(company, json_file)
                    json_file.write("\n")
                    fetched_records += 1
        except Exception as e:
            # Save the uncompleted file so the next run can resume from it
            json_file.flush()
            RneFluxFile.upload(json_file_path, start_date)
            raise Exception(f"Error occurred during the API request: {e}") from e

    logger.info(f"Fetched {fetched_records} records for {start_date}.")
    RneFluxFile.upload(json_file_path, start_date)


@task
def get_every_day_flux():
    """
    Fetches daily flux data from the Registre National des Entreprises (RNE) API
    and saves it to JSON files for a range of dates. This function iterates through
    a date range and calls the `get_and_save_daily_flux_rne` function for each day.
    """
    start_date = date.fromisoformat(compute_start_date())
    end_date = datetime.now(UTC).date() - timedelta(days=1)
    logger.info(f"Start date: {start_date}")
    logger.info(f"End date: {end_date}")

    current_date = start_date
    while current_date <= end_date:
        next_day = current_date + timedelta(days=1)
        # Try to resume only the first day of the loop
        get_and_save_daily_flux_rne(
            current_date.isoformat(),
            next_day.isoformat(),
            resume_from_saved_file=current_date == start_date,
        )
        current_date = next_day

    success_message = (
        "Données flux RNE mises à jour."
        f"<ul><li>Date début flux : {start_date}.</li>"
        f"<li>Date fin flux : {end_date}.</li></ul>"
    )
    ti = get_current_context()["ti"]
    ti.xcom_push(key=Notification.notification_xcom_key, value=success_message)
