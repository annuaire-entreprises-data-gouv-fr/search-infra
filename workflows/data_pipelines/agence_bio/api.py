from typing import Any

from dag_datalake_sirene.helpers.api_client import ApiClient


class BioApiClient(ApiClient):
    BASE_URL = "https://opendata.agencebio.org/api/gouv/operateurs/"
    BATCH_SIZE = 1000

    def __init__(self) -> None:
        super().__init__(base_url=self.BASE_URL)

    def process_response_and_pagination(
        self,
        response: dict[str, Any] = {},
        current_params: dict[str, Any] | None = None,
    ) -> tuple[list[dict[str, Any]] | None, dict[str, Any] | None]:
        if current_params is None:
            initial_params = {"nb": f"{self.BATCH_SIZE}", "debut": "0"}
            return None, initial_params

        data = response.get("items", [])
        if not data:
            return data, None

        new_params = {
            **current_params,
            "debut": str(int(current_params["debut"]) + self.BATCH_SIZE),
        }
        return data, new_params

    def call_api_bio(self) -> list[dict[str, Any]]:
        return self.fetch_all(
            endpoint="",
            response_and_pagination_handler=self.process_response_and_pagination,
            batch_size=self.BATCH_SIZE,
            sleep_time=0.5,
        )
