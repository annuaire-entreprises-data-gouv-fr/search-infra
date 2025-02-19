import functools
import logging
import time
from typing import Any, Callable, ParamSpec, TypeVar

from requests import RequestException, Response, Session

P = ParamSpec("P")
R = TypeVar("R", bound=Response)


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
                            logging.info(f"Status code : {code}")
                            return response
                        case 429 | 502 | 503 | 504:
                            sleep_time = backoff_factor * (2**retries)
                            logging.warning(
                                f"Retryable error: {response.status_code}. "
                                f"Sleeping for {sleep_time} seconds..."
                            )
                            time.sleep(sleep_time)
                        case 500:
                            logging.error(
                                "Internal Server Error (500). "
                                "Terminating retry attempts."
                            )
                            break
                        case _:
                            response.raise_for_status()
                except RequestException as e:
                    logging.error(f"Request failed: {e}")
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
                logging.info(f"Request count: {request_count}")

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
