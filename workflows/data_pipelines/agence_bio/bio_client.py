from typing import Any
from dag_datalake_sirene.helpers.api_client import APIClient


class BIOAPIClient(APIClient):

    def __init__(self):
        super().__init__(base_url="https://opendata.agencebio.org/api/gouv/operateurs/")

    def bio_pagination_handler(self, response=None, current_params=None):
        if not current_params:
            departments = (
                "01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16,17,18,19,21,22,"
                "23,24,25,26,27,28,29,2A,2B,30,31,32,33,34,35,36,37,38,39,40,"
                "41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,"
                "61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,"
                "81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,971,972,973,974,"
                "976,977,978,984,986,987,988,989"
            )
            initial_params = {"departements": departments, "nb": "1000", "debut": "0"}
            return None, initial_params

        data = response.get("items", [])

        if not data:
            return data, None
        else:
            new_params = current_params.copy()
            new_params["debut"] = str(int(current_params["debut"]) + 1000)
            return data, new_params

    def call_api_bio(self) -> list[dict[str, Any]]:
        return self.fetch_all(
            endpoint="",
            pagination_handler=self.bio_pagination_handler,
            batch_size=1000,
            sleep_time=0.5,
        )
