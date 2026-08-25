import functools
import logging
import time
from collections.abc import Callable
from io import BytesIO
from pathlib import Path
from typing import Any, ParamSpec, TypeVar

import pandas as pd
from airflow.sdk import Variable
from requests import RequestException, Response, Session

P = ParamSpec("P")
R = TypeVar("R", bound=Response)

logger = logging.getLogger(__name__)


def retry_request(
    max_retries: int = 10, backoff_factor: float = 0.3
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """
    A decorator that retries a request function on certain HTTP status codes.

    Args:
        max_retries (int): Maximum number of retries before giving up. Default is 3.
        backoff_factor (float): Factor to apply between attempts. Default is 0.3.

    Returns:
        Callable: A decorator function.

    The decorator will retry the request on status codes 429, 502, 503, and 504.
    It will use exponential backoff between retries and log the retry attempts.
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            retries = 0
            while retries <= max_retries:
                try:
                    response = func(*args, **kwargs)
                    match response.status_code:
                        case code if code < 400:
                            logger.info(f"Status code : {code}")
                            return response
                        case 429 | 502 | 503 | 504:
                            sleep_time = backoff_factor * (2**retries)
                            logger.warning(
                                f"Retryable error: {response.status_code}. "
                                f"Sleeping for {sleep_time} seconds..."
                            )
                            time.sleep(sleep_time)
                        case 500:
                            logger.error(
                                "Internal Server Error (500). "
                                "Terminating retry attempts."
                            )
                            break
                        case _:
                            response.raise_for_status()
                except RequestException as e:
                    logger.error(f"Request failed: {e}")
                    if retries == max_retries:
                        raise
                retries += 1
            raise Exception("Max retries exceeded or non-retryable error")

        return wrapper

    return decorator


class ApiClient:
    """
    A client for making API requests with retry functionality.

    This client manages a session for making HTTP requests and provides
    methods for GET requests and paginated data fetching.
    """

    def __init__(self, base_url: str, headers: dict[str, str] | None = None):
        """
        Initialize the ApiClient.

        Args:
            base_url (str): The base URL for all API requests.
            headers (dict[str, str] | None): Optional headers to include in
            all requests.
        """
        self.base_url = base_url
        self.session = Session()
        if headers:
            self.session.headers.update(headers)

    @retry_request()
    def get(self, endpoint: str, params: dict[str, Any] | None = None) -> Response:
        """
        Make a GET request to the specified endpoint.

        Args:
            endpoint (str): The API endpoint to request.
            params (dict[str, Any] | None): Optional query parameters.

        Returns:
            requests.Response: The response from the API.

        This method is decorated with retry_request for automatic retries.
        """
        url = f"{self.base_url}{endpoint}"
        return self.session.get(url, params=params)

    def fetch_all(
        self,
        endpoint: str,
        response_and_pagination_handler: Callable,
        batch_size: int = 1000,
        sleep_time: float = 2.0,
    ) -> list[dict[str, Any]]:
        """
        Fetch all paginated data from an API endpoint.

        Args:
            endpoint (str): The API endpoint to request.
            response_and_pagination_handler (Callable): A function to handle pagination
                and data extraction.
            batch_size (int): Number of items to request per batch. Default is 1000.
            sleep_time (float): Time to sleep between requests in seconds.
                Default is 2.0.

        Returns:
            list[dict[str, Any]]: A list of all data items fetched from the API.
        """
        all_data: list[dict[str, Any]] = []
        request_count = 0
        _, current_params = response_and_pagination_handler()

        while current_params is not None:
            request_count += batch_size
            if request_count % 10000 == 0:
                logger.info(f"Request count: {request_count}")

            start_time = time.time()
            response = self.get(endpoint, params=current_params)
            response_time = time.time() - start_time

            response_json = response.json()

            data, current_params = response_and_pagination_handler(
                response_json, current_params
            )

            all_data.extend(data)

            time.sleep(max(0, sleep_time - response_time))

        return all_data


class AirflowApiClient(ApiClient):
    """Specialized API client for Airflow REST API with authentication and token management."""

    def __init__(self):
        """Initialize the Airflow API client with authentication."""
        base_url = f"http://{Variable.get('AIRFLOW_API_BASE_URL')}"
        super().__init__(f"{base_url}/api/v2")
        self.url_token = f"{base_url}/auth/token"
        self._fetch_and_set_token()

    def get_task_instances(self, dag_id: str, run_id: str) -> list[dict[str, Any]]:
        """Get task instances for a specific DAG run.

        Args:
            dag_id: The DAG ID
            run_id: The DAG run ID

        Returns:
            List of task instances sorted by end date
        """
        endpoint = f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances"

        try:
            response = self.get(endpoint)
            response_json = response.json()

            return sorted(
                response_json["task_instances"],
                key=lambda ti: (ti.get("end_date") is None, ti.get("end_date", "")),
            )
        except KeyError as e:
            logger.error("Unexpected API response format: %s", e)
            raise RuntimeError("Airflow API returned unexpected response format") from e

    def _fetch_and_set_token(self) -> None:
        """Fetch a new authentication token and set it in the session headers."""

        credentials = {
            "username": Variable.get("AIRFLOW_DATAENG_API_USER"),
            "password": Variable.get("AIRFLOW_DATAENG_API_USER_PASSWORD"),
        }

        try:
            # Use the parent class's get method for the auth request
            # We need to temporarily remove auth headers since we're getting a token
            original_headers = self.session.headers
            self.session.headers.clear()

            response = self.session.post(self.url_token, json=credentials, timeout=30)
            response.raise_for_status()

            self._token = response.json()["access_token"]

            self.session.headers.update(
                {"Authorization": f"Bearer {self._token}", **original_headers}
            )

            logger.info("Successfully fetched new Airflow API token")

        except Exception as e:
            logger.error("Failed to fetch Airflow API token: %s", e)
            raise RuntimeError("Failed to authenticate with Airflow API") from e


class GristApiClient(ApiClient):
    """Specialized API client for Grist REST API with token auth."""

    BASE_URL = "https://grist.numerique.gouv.fr"

    def __init__(self, api_token: str) -> None:
        super().__init__(
            base_url=self.BASE_URL,
            headers={"Authorization": f"Bearer {api_token}"},
        )

    def download_table(
        self,
        document_id: str,
        table_id: str,
        output_path: str | Path,
    ) -> None:
        """Download a table from Grist and save it to a CSV file."""
        response = self.get(
            endpoint=f"/api/docs/{document_id}/download/csv",
            params={"tableId": table_id, "header": "colId"},
        )

        df = pd.read_csv(
            BytesIO(response.content),
            dtype=str,
            keep_default_na=False,
        )
        df.columns = df.columns.str.lower()

        df.to_csv(output_path, index=False)
