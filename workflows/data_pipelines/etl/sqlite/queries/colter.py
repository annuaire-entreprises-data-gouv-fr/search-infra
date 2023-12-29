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
