echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" >> .env
more .envExample >> .env
