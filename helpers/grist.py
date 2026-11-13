from io import BytesIO
from typing import Any

import pandas as pd
from airflow.sdk import Variable

from data_pipelines_annuaire.helpers.api_client import ApiClient


class _GristClient(ApiClient):
    def __init__(self) -> None:
        super().__init__(
            base_url="https://grist.numerique.gouv.fr",
            headers={"Authorization": f"Bearer {Variable.get('GRIST_API_TOKEN')}"},
        )


class GristTable:
    """
    A table of a Grist document.

    Example:
        > table = GristTable("<document_id>", "Ma_Table")
        > df = table.read()
        > ...
        > table.append([{"user": "1"}])
    """

    def __init__(self, document_id: str, table_id: str) -> None:
        self.document_id = document_id
        self.table_id = table_id
        self._client: _GristClient = _GristClient()

    def read(self) -> pd.DataFrame:
        """
        Return the rows with the column names in lowercase as header.
        Every cell is a string and a blank cell is an empty string.
        """
        response = self._client.get(
            endpoint=f"/api/docs/{self.document_id}/download/csv",
            params={"tableId": self.table_id, "header": "colId"},
        )
        df = pd.read_csv(BytesIO(response.content), dtype=str, keep_default_na=False)
        df.columns = df.columns.str.lower()
        return df

    def append(self, records: list[dict[str, Any]]) -> None:
        """Add rows at the end of the table. The dict keys are the column names."""
        if not records:
            return
        self._client.post(
            endpoint=f"/api/docs/{self.document_id}/tables/{self.table_id}/records",
            json={"records": [{"fields": record} for record in records]},
        )
