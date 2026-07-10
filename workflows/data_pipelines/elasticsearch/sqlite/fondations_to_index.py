"""
Les fondations avec des SIRET sont indexées en même temps que les données entreprises.
Ici la requête permet d'ajouter ensuite toutes les fondations sans SIRET.
Celles-ci auront donc l'objet `unite_legale` absent.
"""

select_fondations_to_index_query = """
    SELECT
        numero_rnf,
        titre,
        type_organisme,
        date_creation,
        adresse,
        code_postal,
        ville
    FROM
        fondation
    WHERE siret IS NULL
    ;
"""
