create_table_colter_query = """
     CREATE TABLE IF NOT EXISTS colter
     (
         siren,
         colter_code,
         colter_code_insee,
         colter_niveau
     )
"""

create_table_elus_query = """
     CREATE TABLE IF NOT EXISTS elus
     (
         siren,
         nom,
         prenom,
         date_naissance,
         sexe,
         fonction
     )
    """

delete_duplicates_elus_query = """
    DELETE FROM elus
    WHERE ROWID NOT IN
    (
        SELECT MIN(ROWID)
        FROM elus
        GROUP BY siren, nom, prenom, date_naissance, sexe, fonction
    )
    """
