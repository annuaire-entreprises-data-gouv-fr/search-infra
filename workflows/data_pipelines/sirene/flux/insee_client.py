from typing import Any
from dag_datalake_sirene.helpers.api_client import APIClient


class INSEEAPIClient(APIClient):
    def __init__(self, api_endpoint: str, bearer_token: str):
        super().__init__(
            base_url=api_endpoint, headers={"Authorization": f"Bearer {bearer_token}"}
        )

    def call_insee_api(self, endpoint: str, data_property: str) -> list[dict[str, Any]]:
        def next_cursor(response: dict[str, Any]) -> str | None:
            header = response.get("header", {})
            next_cursor = header.get("curseurSuivant")
            current_cursor = header.get("curseur")
            return (
                None
                if not next_cursor or next_cursor == current_cursor
                else next_cursor
            )

        return self.fetch_all(
            endpoint=endpoint,
            params={},
            cursor_param="curseur",
            data_property=data_property,
            next_cursor_func=next_cursor,
            batch_size=1000,
            sleep_time=2.0,
        )
