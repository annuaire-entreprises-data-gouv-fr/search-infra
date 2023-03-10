select_sitemap_fields_query = """SELECT
        ul.siren,
        ul.nom_raison_sociale as nom_raison_sociale,
        ul.sigle as sigle,
        ul.etat_administratif_unite_legale as etat_administratif_unite_legale,
        ul.nature_juridique_unite_legale as nature_juridique_unite_legale,
        st.code_postal as code_postal,
        ul.activite_principale_unite_legale as activite_principale_unite_legale
        FROM
            unite_legale ul
        JOIN
            siretsiege st
        ON st.siren = ul.siren;"""  # noqa
