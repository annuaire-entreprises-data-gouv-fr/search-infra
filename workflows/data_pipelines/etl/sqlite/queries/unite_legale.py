create_table_unite_legale_query = """
        CREATE TABLE IF NOT EXISTS unite_legale
        (
            siren,
            date_creation_unite_legale,
            sigle,
            prenom,
            identifiant_association_unite_legale,
            tranche_effectif_salarie_unite_legale,
            date_mise_a_jour_unite_legale,
            categorie_entreprise,
            etat_administratif_unite_legale,
            nom,
            nom_usage,
            nom_raison_sociale,
            denomination_usuelle_1,
            denomination_usuelle_2,
            denomination_usuelle_3,
            nature_juridique_unite_legale,
            activite_principale_unite_legale,
            economie_sociale_solidaire_unite_legale,
            statut_diffusion_unite_legale,
            est_societe_mission,
            annee_categorie_entreprise,
            annee_tranche_effectif_salarie,
            caractere_employeur
        )
    """


create_table_flux_unite_legale_query = """
        CREATE TABLE IF NOT EXISTS flux_unite_legale
        (
            siren,
            date_creation_unite_legale,
            sigle,
            prenom,
            identifiant_association_unite_legale,
            tranche_effectif_salarie_unite_legale,
            date_mise_a_jour_unite_legale,
            categorie_entreprise,
            etat_administratif_unite_legale,
            nom,
            nom_usage,
            nom_raison_sociale,
            denomination_usuelle_1,
            denomination_usuelle_2,
            denomination_usuelle_3,
            nature_juridique_unite_legale,
            activite_principale_unite_legale,
            economie_sociale_solidaire_unite_legale,
            statut_diffusion_unite_legale,
            est_societe_mission,
            annee_categorie_entreprise,
            annee_tranche_effectif_salarie,
            caractere_employeur
        )
    """

replace_table_unite_legale_query = """
        REPLACE INTO unite_legale
        (
            siren,
            date_creation_unite_legale,
            sigle,
            prenom,
            identifiant_association_unite_legale,
            tranche_effectif_salarie_unite_legale,
            date_mise_a_jour_unite_legale,
            categorie_entreprise,
            etat_administratif_unite_legale,
            nom,
            nom_usage,
            nom_raison_sociale,
            denomination_usuelle_1,
            denomination_usuelle_2,
            denomination_usuelle_3,
            nature_juridique_unite_legale,
            activite_principale_unite_legale,
            economie_sociale_solidaire_unite_legale,
            statut_diffusion_unite_legale,
            est_societe_mission,
            annee_categorie_entreprise,
            annee_tranche_effectif_salarie,
            caractere_employeur
        ) SELECT
            a.siren,
            a.date_creation_unite_legale,
            a.sigle,
            a.prenom,
            a.identifiant_association_unite_legale,
            a.tranche_effectif_salarie_unite_legale,
            a.date_mise_a_jour_unite_legale,
            a.categorie_entreprise,
            a.etat_administratif_unite_legale,
            a.nom,
            a.nom_usage,
            a.nom_raison_sociale,
            a.denomination_usuelle_1,
            a.denomination_usuelle_2,
            a.denomination_usuelle_3,
            a.nature_juridique_unite_legale,
            a.activite_principale_unite_legale,
            a.economie_sociale_solidaire_unite_legale,
            a.statut_diffusion_unite_legale,
            a.est_societe_mission,
            a.annee_categorie_entreprise,
            a.annee_tranche_effectif_salarie,
            a.caractere_employeur
        FROM flux_unite_legale a LEFT JOIN unite_legale b
        ON a.siren = b.siren
    """
