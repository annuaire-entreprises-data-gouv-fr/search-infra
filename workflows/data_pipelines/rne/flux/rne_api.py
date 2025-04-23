import logging
import random
import time
from typing import Union

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import SSLError

from dag_datalake_sirene.config import RNE_API_DIFF_URL, RNE_API_TOKEN_URL, RNE_AUTH


class ApiRNEClient:
    """API client for interacting with the
    Registre National des Entreprises (RNE) API."""

    def __init__(self, max_retries=100):
        """
        Initializes the API client.

        Attributes:
            auth (list[dict]): List of authentication data.
            session (requests.Session): HTTP session with a custom adapter.
            token (str): The API token used for authentication.
            max_retries (int): Maximum number of retries for API requests.
        """
        self.auth = RNE_AUTH
        self.session = self.create_persistent_session()
        self.token = self.get_new_token()
        self.max_retries = max_retries

    def create_persistent_session(self):
        """Create a session with a custom HTTP adapter for max retries."""
        session = requests.Session()
        adapter = HTTPAdapter(max_retries=20)
        session.mount("http://", adapter)
        return session

    def get_new_token(self) -> Union[str, None]:
        """
        Gets a new access token from the RNE API.

        Returns:
            Union[str, None]: The access token if successful, otherwise None.
        """
        try:
            selected_auth = random.choice(self.auth)
            logging.info(f"Authentification account used: {selected_auth['username']}")
            response = self.session.post(RNE_API_TOKEN_URL, json=selected_auth)
            response.raise_for_status()
            token = response.json()["token"]
            logging.info("New token received...")
            return token
        except SSLError as err:
            logging.warning(f"Unexpected EOF occurred in violation of protocol: {err}")
            time.sleep(600)
        except Exception as err:
            logging.error(f"An error occurred when trying to get a new token: {err}")
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

        for attempt in range(self.max_retries + 1):
            if attempt > 0:
                logging.info(f"Making API call try : {attempt}")
            try:
                if not self.token:
                    logging.info("Getting new token...")
                    self.token = self.get_new_token()
                headers = {"Authorization": f"Bearer {self.token}"}
                response = self.session.get(url, headers=headers)
                response.raise_for_status()
                response = response.json()
                last_siren = self.get_last_siren_in_page(response)
                logging.info(f"%%%%%%% LAST SIREN : {last_siren}")
                return response, last_siren

            except Exception as e:
                if hasattr(e, "response") and e.response.status_code in [401, 403, 429]:
                    self.token = self.get_new_token()
                    logging.info("Got a new access token and retrying...")
                elif hasattr(e, "response") and e.response.status_code == 500:
                    if "Allowed memory size of" in str(e.response.content):
                        url = url.replace("pageSize=100", "pageSize=1")
                        logging.info(f"***Memory Error changing page size to 1 : {url}")
                    else:
                        logging.info(f"***Error HTTP: {e}")
                        url = url.replace("pageSize=100", "pageSize=5")
                        logging.info(f"***Changing page size to 5: {url}")
                        time.sleep(60)
                else:
                    logging.error(f"Error occurred while making API request: {e}")
                    if attempt < self.max_retries:
                        time.sleep(60)
                    else:
                        raise Exception(
                            "Max retries reached. Unable to establish a connection."
                        )
