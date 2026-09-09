import logging
import random
import time

import requests
from requests.exceptions import SSLError

from data_pipelines_annuaire.config import RNE_API_DIFF_URL, RNE_API_TOKEN_URL, RNE_AUTH
from data_pipelines_annuaire.helpers.api_client import API_TIMEOUT
from data_pipelines_annuaire.helpers.retry import BASE_DELAY, retry_delay

logger = logging.getLogger(__name__)

# The RNE API quota is bound to an account, only time will lift a 429
RATE_LIMITED_BASE_DELAY = 5 * 60


class ApiRNEClient:
    """API client for interacting with the
    Registre National des Entreprises (RNE) API."""

    def __init__(self, max_retries=8):
        """
        Initializes the API client.

        Attributes:
            auth (list[dict]): List of authentication data.
            session (requests.Session): HTTP session with a custom adapter.
            token (str): The API token used for authentication.
            max_retries (int): Maximum number of retries for API requests.
        """
        self.auth = RNE_AUTH
        self.session = requests.Session()
        self.token = self.get_new_token()
        self.max_retries = max_retries

    def get_new_token(self) -> str | None:
        """
        Gets a new access token from the RNE API.

        Returns:
            Union[str, None]: The access token if successful, otherwise None.
        """
        try:
            selected_auth = random.choice(self.auth)
            logger.info(f"Authentification account used: {selected_auth['username']}")
            response = self.session.post(
                RNE_API_TOKEN_URL, json=selected_auth, timeout=API_TIMEOUT
            )
            response.raise_for_status()
            token = response.json()["token"]
            logger.info("New token received...")
            return token
        except SSLError as err:
            logger.warning(f"Unexpected EOF occurred in violation of protocol: {err}")
            time.sleep(600)
        except Exception as err:
            logger.error(f"An error occurred when trying to get a new token: {err}")
        return None

    def get_last_siren_in_page(self, page_data):
        """
        Extracts the last SIREN number from the page data.
        """
        return page_data[-1].get("company", {}).get("siren") if page_data else None

    def make_api_request(self, start_date, end_date, last_siren=None):
        """
        Makes an API request and retries it up to max_retries times if it fails.

        Args:
            start_date (str): The start date for the API request.
            end_date (str): The end date for the API request.
            last_siren (Optional[str]): The last SIREN number from a previous request.

        Returns:
            Tuple[dict, Optional[str]]: A tuple containing the API
            response and the last SIREN number.
        """

        url = f"{RNE_API_DIFF_URL}from={start_date}&to={end_date}&pageSize=100"
        if last_siren:
            url += f"&searchAfter={last_siren}"

        waits = 0
        for attempt in range(self.max_retries + 1):
            if attempt > 0:
                logger.info(f"Making API call try : {attempt}")
            try:
                if not self.token:
                    logger.info("Getting new token...")
                    self.token = self.get_new_token()
                headers = {"Authorization": f"Bearer {self.token}"}
                response = self.session.get(url, headers=headers, timeout=API_TIMEOUT)
                response.raise_for_status()
                response = response.json()
                last_siren = self.get_last_siren_in_page(response)
                if last_siren is None:
                    logger.info(
                        "Empty page : every SIREN updated between "
                        f"{start_date} and {end_date} has been fetched."
                    )
                else:
                    logger.info(f"LAST SIREN : {last_siren}")
                return response, last_siren

            except Exception as e:
                error_response = getattr(e, "response", None)
                status_code = getattr(error_response, "status_code", None)
                body = getattr(error_response, "text", "") or ""
                logger.error(
                    f"API request failed on {url} "
                    f"with status {status_code}: {e}. Response: {body[:500]}"
                )
                base_delay = BASE_DELAY
                if status_code == 429:
                    logger.warning("Rate limited by the RNE API.")
                    base_delay = RATE_LIMITED_BASE_DELAY
                elif status_code in [401, 403]:
                    self.token = self.get_new_token()
                    logger.info("Got a new access token.")
                elif status_code == 500:
                    if "Allowed memory size of" in str(error_response.content):
                        url = url.replace("pageSize=100", "pageSize=1")
                        logger.info(f"***Memory Error changing page size to 1 : {url}")
                    else:
                        url = url.replace("pageSize=100", "pageSize=5")
                        logger.info(f"***Changing page size to 5: {url}")

                delay = retry_delay(waits, base_delay=base_delay)
                waits += 1
                logger.info(f"Waiting {delay:.1f} seconds before the next try...")
                time.sleep(delay)

        raise Exception(f"Max retries reached ({self.max_retries})")
