# Dag permettant l'alimentation d'un datalake sirene

Le notebook `preprocess-data-before-indexation` permet de formater les données de la base sirene pour optimiser son
indexation et faciliter le requêtage elasticsearch.

Le script `dag-insert-elk-sirene.py` est un DAG airflow orchestrant un ensemble de tâche permettant :

- de formatter les données de la base sirene
- de créer un index elasticsearch
- d'insérer des données dans cet index elasticsearch (index siren)
