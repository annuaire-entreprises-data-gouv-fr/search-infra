FROM apache/airflow

USER root 

ARG AIRFLOW_HOME=/opt/airflow 

ADD dags /opt/airflow/dags 

ADD airflow.cfg /opt/airflow/airflow.cfg

RUN pip install --upgrade pip 

RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 467B942D3A79BD29

RUN apt-get update -y
RUN apt-get install git -y

RUN chown -R "airflow:root" /opt/airflow/

ADD ssh /home/airflow/.ssh/
RUN chown -R airflow:root /home/airflow/.ssh

USER airflow 

RUN pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org boto3


# USER ${AIRFLOW_UID}
USER airflow

ADD requirements.txt /requirements.txt

RUN pip install -r /requirements.txt

RUN git config --global user.email "geoffrey.aldebert@data.gouv.fr"
RUN git config --global user.name "Geoffrey Aldebert (Bot Airflow)"

