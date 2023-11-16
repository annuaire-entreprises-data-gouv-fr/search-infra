import pandas as pd
from dag_datalake_sirene.elasticsearch.data_enrichment import map_roles


def preprocess_dirigeant_pm(query):
    cols = [column[0] for column in query.description]
    dirig_chunk = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)
    dirig_chunk.sort_values(
        by=[
            "siren",
            "date_mise_a_jour",
            "denomination",
            "siren_dirigeant",
            "role",
            "forme_juridique",
        ],
        inplace=True,
        ascending=[True, True, False, False, False, False],
    )
    dirig_chunk.drop_duplicates(
        subset=["siren", "date_mise_a_jour", "denomination", "siren_dirigeant", "role"],
        keep="first",
        inplace=True,
    )
    dirig_chunk["denomination"] = dirig_chunk["denomination"].str.upper()

    # Map role numbers to descriptions
    dirig_chunk["role_description"] = map_roles(dirig_chunk["role"])

    dirig_clean = (
        dirig_chunk.groupby(by=["siren", "siren_dirigeant", "denomination"])[
            "role_description"
        ]
        .apply(lambda x: ", ".join(str(val) for val in x if val is not None))
        .reset_index()
    )
    return dirig_clean
