select_sitemap_fields_query = """SELECT
        ul.nom_raison_sociale as nom_raison_sociale,
        ul.denomination_usuelle_1 as denomination_usuelle_1_unite_legale,
        ul.denomination_usuelle_2 as denomination_usuelle_2_unite_legale,
        ul.denomination_usuelle_3 as denomination_usuelle_3_unite_legale,
        ul.sigle as sigle,
        ul.siren as siren,
        ul.etat_administratif_unite_legale as etat_administratif_unite_legale,
        ul.nature_juridique_unite_legale as nature_juridique_unite_legale,
        st.code_postal as code_postal,
        st.commune as code_commune,
        st.code_pays_etranger as code_pays_etranger,
        st.nom_commercial as nom_commercial,
        ul.activite_principale_unite_legale as activite_principale_unite_legale,
        ul.statut_diffusion_unite_legale as statut_diffusion_unite_legale,
        ul.nature_juridique_unite_legale
        FROM
            unite_legale ul
        JOIN
            siege st
        ON st.siren = ul.siren;"""  # noqa
