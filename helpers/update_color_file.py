import json
import logging

from dag_datalake_sirene.helpers.minio_helpers import minio_client


def update_color_file(**kwargs):
    next_color = kwargs["ti"].xcom_pull(key="next_color", task_ids="get_colors")
    current_color = kwargs["ti"].xcom_pull(key="current_color", task_ids="get_colors")
    colors = {"CURRENT_COLOR": next_color, "NEXT_COLOR": current_color}
    logging.info(f"******************** Next color configuration: {colors}")

    with open("colors.json", "w") as write_file:
        json.dump(colors, write_file)

    minio_client.send_files(
        list_files=[
            {
                "source_path": "",
                "source_name": "colors.json",
                "dest_path": "",
                "dest_name": "colors.json",
                "content_type": "application/json",
            }
        ],
    )
