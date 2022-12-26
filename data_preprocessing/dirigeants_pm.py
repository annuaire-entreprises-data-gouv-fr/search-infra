import pandas as pd


def preprocess_dirigeant_pm(query):
    cols = [column[0] for column in query.description]
    rep_chunk = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)
    rep_chunk.sort_values(
        by=["siren", "siren_pm", "denomination", "sigle", "qualite"],
        inplace=True,
        ascending=[True, False, False, False, False],
    )
    rep_chunk.drop_duplicates(
        subset=["siren", "siren_pm", "denomination", "sigle", "qualite"],
        keep="first",
        inplace=True,
    )
    rep_clean = (
        rep_chunk.groupby(by=["siren", "siren_pm", "denomination", "sigle"])["qualite"]
        .apply(lambda x: ", ".join(x))
        .reset_index()
    )
    return rep_clean
