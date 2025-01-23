from dag_datalake_sirene.config import (
    DataSourceConfig,
)
from airflow.models import Variable

RNE_STOCK_CONFIG = DataSourceConfig(
    name="rne-stock",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/rne/stock",
    minio_path="rne/stock",
    files_to_download={
        "ftp": {
            "url": Variable.get("RNE_FTP_URL", ""),
        },
    },
)
