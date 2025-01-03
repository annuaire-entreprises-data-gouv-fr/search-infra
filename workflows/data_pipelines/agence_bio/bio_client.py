from typing import Any
from dag_datalake_sirene.helpers.api_client import ApiClient


class BioApiClient(ApiClient):
    BASE_URL = "https://opendata.agencebio.org/api/gouv/operateurs/"
    DEPARTMENTS = (
        "01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16,17,18,19,21,22,"
        "23,24,25,26,27,28,29,2A,2B,30,31,32,33,34,35,36,37,38,39,40,"
        "41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,"
        "61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,"
        "81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,971,972,973,974,"
        "976,977,978,984,986,987,988,989"
    )

    def __init__(self):
        super().__init__(base_url=self.BASE_URL)

    def process_response_and_pagination(
        self, response: dict[str, Any] = None, current_params: dict[str, Any] = None
    ) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
        if current_params is None:
            initial_params = {**self.params, "debut": "0"}
            return None, initial_params

        data = response.get("items", [])
        if not data:
            return data, None

        new_params = {
            **current_params,
            "debut": str(int(current_params["debut"]) + 1000),
        }
        return data, new_params

    def call_api_bio(self) -> list[dict[str, Any]]:
        self.params = {"departements": self.DEPARTMENTS, "nb": "1000"}
        return self.fetch_all(
            endpoint="",
            response_and_pagination_handler=self.process_response_and_pagination,
            batch_size=1000,
            sleep_time=0.5,
        )
