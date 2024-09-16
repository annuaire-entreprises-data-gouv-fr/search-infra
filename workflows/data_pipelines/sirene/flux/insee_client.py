from typing import Any
from helpers.api_client import APIClient


class INSEEAPIClient(APIClient):
    def __init__(self, api_endpoint: str, bearer_token: str):
        super().__init__(
            base_url=api_endpoint, headers={"Authorization": f"Bearer {bearer_token}"}
        )

    def process_response_and_pagination(
        self, response: dict[str, Any] = None, current_params: dict[str, Any] = None
    ) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
        if current_params is None:
            initial_params = {"curseur": "*"}
            return None, initial_params

        header = response.get("header", {})
        next_cursor = header.get("curseurSuivant")
        current_cursor = header.get("curseur")

        data = response.get(self.data_property, [])

        if not next_cursor or next_cursor == current_cursor:
            return data, None

        new_params = {**current_params, "curseur": next_cursor}
        return data, new_params

    def call_insee_api(self, endpoint: str, data_property: str) -> list[dict[str, Any]]:
        self.data_property = data_property
        return self.fetch_all(
            endpoint=endpoint,
            response_and_pagination_handler=self.process_response_and_pagination,
            batch_size=1000,
            sleep_time=2.0,
        )
