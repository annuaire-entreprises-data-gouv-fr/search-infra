import pandas as pd
from dag_datalake_sirene.elasticsearch.data_enrichment import map_roles


def preprocess_dirigeants_pp(query):
    cols = [column[0] for column in query.description]
    dirig_chunk = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)
    dirig_chunk.sort_values(
        by=[
            "siren",
            "nom",
            "nom_usage",
            "prenoms",
            "role",
            "date_mise_a_jour",
            "date_de_naissance",
            "nationalite",
        ],
        inplace=True,
        ascending=[True, True, True, True, True, True, False, False],
    )

    dirig_chunk.drop_duplicates(
        subset=[
            "siren",
            "nom",
            "nom_usage",
            "prenoms",
            "date_de_naissance",
            "role",
            "date_mise_a_jour",
            "nationalite",
        ],
        keep="first",
        inplace=True,
    )

    dirig_chunk = dirig_chunk.applymap(lambda x: str(x).upper())

    # Map role numbers to descriptions
    dirig_chunk["role_description"] = map_roles(dirig_chunk["role"])

    dirig_clean = (
        dirig_chunk.groupby(
            [
                "siren",
                "nom",
                "prenoms",
                "date_de_naissance",
                "nationalite",
                "date_mise_a_jour",
            ]
        )["role_description"]
        .apply(lambda x: ", ".join(str(val) for val in x if val is not None))
        .reset_index()
    )
    return dirig_clean
