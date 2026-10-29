import pandas as pd

# fmt: off
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.data_enrichment import (
    map_roles,
)

# fmt: on


def preprocess_personne_physique(query):
    cols = [column[0] for column in query.description]
    pp_chunk = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)

    for column in ["nom", "nom_usage", "prenoms"]:
        pp_chunk[column] = pp_chunk[column].str.upper()

    pp_chunk["role_description"] = map_roles(pp_chunk["role"])
    return pp_chunk


def preprocess_dirigeant_pm(query):
    cols = [column[0] for column in query.description]
    dirig_chunk = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)

    dirig_chunk["denomination"] = dirig_chunk["denomination"].str.upper()

    dirig_chunk["role_description"] = map_roles(dirig_chunk["role"])
    return dirig_chunk
