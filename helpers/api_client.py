import functools
import time
import logging
from typing import Callable, TypeVar, ParamSpec, Any
from requests import Response, RequestException
import requests

P = ParamSpec("P")
R = TypeVar("R", bound=Response)


def retry_request(
    max_retries: int = 3, backoff_factor: float = 0.3
) -> Callable[[Callable[P, R]], Callable[P, R]]:
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
                                f"""
                                Retryable error: {response.status_code}. Sleeping for
                                {sleep_time} seconds..."""
                            )
                            time.sleep(sleep_time)
                        case 500:
                            logging.error(
                                """
                                Internal Server Error (500). Terminating retry attempts.
                                """
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


class APIClient:
    def __init__(self, base_url: str, headers: dict[str, str] | None = None):
        self.base_url = base_url
        self.session = requests.Session()
        if headers:
            self.session.headers.update(headers)

    @retry_request()
    def get(
        self, endpoint: str, params: dict[str, Any] | None = None
    ) -> requests.Response:
        url = f"{self.base_url}{endpoint}"
        return self.session.get(url, params=params)

    def fetch_all(
        self,
        endpoint: str,
        params: dict[str, Any],
        cursor_param: str,
        cursor: str | None,
        data_property: str,
        next_cursor_func: Callable[[dict[str, Any], str | None], str | None],
        batch_size: int = 1000,
        sleep_time: float = 2.0,
    ) -> list[dict[str, Any]]:
        all_data: list[dict[str, Any]] = []
        request_count = 0

        while cursor is not None:
            request_count += batch_size
            if request_count % 10000 == 0:
                logging.info(f"Request count: {request_count}")

            current_params = params.copy()
            current_params[cursor_param] = str(cursor)

            response = self.get(endpoint, params=current_params)
            response_json = response.json()

            cursor = next_cursor_func(response_json, cursor)

            all_data.extend(response_json.get(data_property, []))

            time.sleep(sleep_time)

        return all_data
