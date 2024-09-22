FROM docker.io/apache/airflow:2.9.0-python3.11

USER root

# Installer sudo et lftp
RUN apt-get update && \
    apt-get install -y sudo lftp && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Donner les permissions sudo à l'utilisateur airflow
RUN echo "airflow ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

USER airflow

COPY requirements.txt .
RUN pip install -r requirements.txt
