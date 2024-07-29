from typing import Any
from dag_datalake_sirene.helpers.api_client import APIClient


class BIOAPIClient(APIClient):

    def __init__(self):
        super().__init__(base_url="https://opendata.agencebio.org/api/gouv/operateurs/")

    def fetch_all_operators(self) -> list[dict[str, Any]]:
        departments = (
            "01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16,17,18,19,21,22,"
            "23,24,25,26,27,28,29,2A,2B,30,31,32,33,34,35,36,37,38,39,40,"
            "41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,"
            "61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,"
            "81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,971,972,973,974,"
            "976,977,978,984,986,987,988,989"
        )

        def next_cursor(response: dict[str, Any]) -> str | None:
            items = response.get("items", [])
            return None if not items else str(int(response.get("debut", 0)) + 1000)

        return self.fetch_all(
            endpoint="",
            params={"departements": departments, "nb": "1000"},
            cursor_param="debut",
            data_property="items",
            next_cursor_func=next_cursor,
            batch_size=1000,
            sleep_time=0.5,
        )
