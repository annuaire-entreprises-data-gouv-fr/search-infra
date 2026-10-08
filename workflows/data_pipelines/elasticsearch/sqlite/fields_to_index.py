SELECT_FIELDS_TO_INDEX_QUERY = """SELECT
            unite_legale.activite_principale_unite_legale,
            unite_legale.activite_principale_naf25_unite_legale,
            unite_legale.caractere_employeur,
            unite_legale.categorie_entreprise,
            unite_legale.date_creation_unite_legale,
            unite_legale.date_fermeture_unite_legale as date_fermeture,
            unite_legale.date_mise_a_jour_insee,
            unite_legale.date_mise_a_jour_rne,
            unite_legale.denomination_usuelle_1 as denomination_usuelle_1_unite_legale,
            unite_legale.denomination_usuelle_2 as denomination_usuelle_2_unite_legale,
            unite_legale.denomination_usuelle_3 as denomination_usuelle_3_unite_legale,
            unite_legale.economie_sociale_solidaire_unite_legale,
            unite_legale.etat_administratif_unite_legale,
            unite_legale.from_insee,
            unite_legale.from_rne,
            unite_legale.identifiant_association_unite_legale,
            unite_legale.nature_juridique_unite_legale,
            unite_legale.nom,
            unite_legale.nom_raison_sociale,
            unite_legale.nom_usage,
            unite_legale.prenom,
            unite_legale.sigle,
            unite_legale.siren,
            doublons.siren_conserve,
            siege.siret as siret_siege,
            unite_legale.tranche_effectif_salarie_unite_legale,
            unite_legale.statut_diffusion_unite_legale,
            unite_legale.est_societe_mission,
            unite_legale.annee_categorie_entreprise,
            unite_legale.annee_tranche_effectif_salarie,
            (SELECT sirets_par_idcc FROM convention_collective WHERE
                        siren = unite_legale.siren) as sirets_par_idcc,
            (SELECT liste_idcc_unite_legale FROM convention_collective WHERE
                        siren = unite_legale.siren) as liste_idcc_unite_legale,
            count_etablissement."count" as nombre_etablissements,
            count_etablissement_ouvert."count" as nombre_etablissements_ouverts,
            CASE WHEN bilan_financier.siren IS NOT NULL THEN json_object(
                'ca', bilan_financier.ca,
                'resultat_net', bilan_financier.resultat_net,
                'date_cloture_exercice', bilan_financier.date_cloture_exercice,
                'annee_cloture_exercice', bilan_financier.annee_cloture_exercice
            ) END as bilan_financier,
            (SELECT json_group_array(
                json_object(
                    'siren', siren,
                    'date_mise_a_jour', date_mise_a_jour,
                    'date_de_naissance', date_de_naissance,
                    'nom', nom,
                    'nom_usage', nom_usage,
                    'prenoms', prenoms,
                    'nationalite', nationalite,
                    'role_description', role_description
                    )
                ) FROM
                (
                    SELECT siren, date_mise_a_jour, date_de_naissance, nom,
                    nom_usage, prenoms, nationalite, role_description
                    FROM dirigeant_pp
                    WHERE siren = unite_legale.siren
                )
            ) as dirigeants_pp,
            (SELECT json_group_array(
                    json_object(
                        'siren', siren,
                        'date_mise_a_jour', date_mise_a_jour,
                        'denomination', denomination,
                        'siren_dirigeant', siren_dirigeant,
                        'role_description', role_description,
                        'forme_juridique', forme_juridique
                        )
                    ) FROM
                    (
                        SELECT siren, date_mise_a_jour, denomination, siren_dirigeant,
                        role_description, forme_juridique
                        FROM dirigeant_pm
                        WHERE siren = unite_legale.siren
                    )
                ) as dirigeants_pm,
            (SELECT json_group_array(
                    json_object(
                        'activite_principale',activite_principale,
                        'activite_principale_naf25',activite_principale_naf25,
                        'activite_principale_registre_metier',
                        activite_principale_registre_metier,
                        'ancien_siege',ancien_siege,
                        'caractere_employeur',caractere_employeur,
                        'cedex',cedex,
                        'code_pays_etranger',code_pays_etranger,
                        'code_postal',code_postal,
                        'commune',commune,
                        'complement_adresse',complement_adresse,
                        'date_creation',date_creation,
                        'date_debut_activite',date_debut_activite,
                        'date_fermeture',date_fermeture,
                        'distribution_speciale',distribution_speciale,
                        'enseigne_1',enseigne_1,
                        'enseigne_2',enseigne_2,
                        'enseigne_3',enseigne_3,
                        'est_siege',est_siege,
                        'etat_administratif',etat_administratif_etablissement,
                        'geo_adresse',geo_adresse,
                        'geo_id',geo_id,
                        'geo_score',geo_score,
                        'indice_repetition',indice_repetition,
                        'latitude',latitude,
                        'libelle_cedex',libelle_cedex,
                        'libelle_commune',libelle_commune,
                        'libelle_commune_etranger',libelle_commune_etranger,
                        'libelle_pays_etranger',libelle_pays_etranger,
                        'libelle_voie',libelle_voie,
                        'liste_finess_geographique',liste_finess_geographique,
                        'liste_id_bio',liste_id_bio,
                        'liste_idcc',liste_idcc,
                        'liste_rge',liste_rge,
                        'liste_uai',liste_uai,
                        'longitude',longitude,
                        'nom_commercial',nom_commercial,
                        'numero_voie',numero_voie,
                        'dernier_numero_voie',dernier_numero_voie,
                        'siren',siren,
                        'siret',siret,
                        'statut_diffusion_etablissement',
                        statut_diffusion_etablissement,
                        'tranche_effectif_salarie',tranche_effectif_salarie,
                        'annee_tranche_effectif_salarie',annee_tranche_effectif_salarie,
                        'date_mise_a_jour_insee',date_mise_a_jour_insee,
                        'date_mise_a_jour_rne',date_mise_a_jour_rne,
                        'type_voie',type_voie,
                        'x',x,
                        'y',y,
                        -- json() is required, not decorative: SQLite only carries the
                        -- JSON subtype of json_group_array() across a subquery boundary
                        -- on some versions. Without it these two come back as escaped
                        -- strings and the transform iterates over their characters.
                        'successions',json_object(
                            'predecesseurs',json(successions_predecesseurs),
                            'successeurs',json(successions_successeurs)
                        )
                        )
                    ) FROM
                    (
                        SELECT
                        etablissement.activite_principale,
                        etablissement.activite_principale_naf25,
                        etablissement.activite_principale_registre_metier,
                        etablissement.ancien_siege,
                        etablissement.caractere_employeur,
                        etablissement.cedex,
                        etablissement.code_pays_etranger,
                        etablissement.code_postal,
                        etablissement.commune,
                        etablissement.complement_adresse,
                        etablissement.date_creation,
                        etablissement.date_debut_activite,
                        etablissement.date_fermeture_etablissement as date_fermeture,
                        etablissement.distribution_speciale,
                        etablissement.enseigne_1,
                        etablissement.enseigne_2,
                        etablissement.enseigne_3,
                        etablissement.est_siege,
                        etablissement.etat_administratif_etablissement,
                        NULL as geo_adresse,
                        NULL as geo_id,
                        NULL as geo_score,
                        etablissement.indice_repetition,
                        etablissement.latitude,
                        etablissement.libelle_cedex,
                        etablissement.libelle_commune,
                        etablissement.libelle_commune_etranger,
                        etablissement.libelle_pays_etranger,
                        etablissement.libelle_voie,
                        etablissement.longitude,
                        finess_geographique.liste_finess_geographique,
                        agence_bio.liste_id_bio,
                        convention_collective.liste_idcc_etablissement as liste_idcc,
                        rge.liste_rge,
                        uai.liste_uai,
                        etablissement.nom_commercial,
                        etablissement.numero_voie,
                        etablissement.dernier_numero_voie,
                        etablissement.siren,
                        etablissement.siret,
                        etablissement.statut_diffusion_etablissement,
                        etablissement.tranche_effectif_salarie,
                        etablissement.annee_tranche_effectif_salarie,
                        etablissement.date_mise_a_jour_insee,
                        etablissement.date_mise_a_jour_rne,
                        etablissement.type_voie,
                        etablissement.x,
                        etablissement.y,
                        (SELECT json_group_array(json_object(
                            'siret', siret_predecesseur,
                            'date_lien_succession', date_lien_succession,
                            'transfert_siege', transfert_siege,
                            'continuite_economique', continuite_economique
                            ))
                        FROM liens_succession
                        WHERE siret_successeur = etablissement.siret
                        ) as successions_predecesseurs,
                        (SELECT json_group_array(json_object(
                            'siret', siret_successeur,
                            'date_lien_succession', date_lien_succession,
                            'transfert_siege', transfert_siege,
                            'continuite_economique', continuite_economique
                            ))
                        FROM liens_succession
                        WHERE siret_predecesseur = etablissement.siret
                        ) as successions_successeurs
                        FROM etablissement
                        LEFT JOIN finess_geographique ON finess_geographique.siret = etablissement.siret
                        LEFT JOIN agence_bio ON agence_bio.siret = etablissement.siret
                        LEFT JOIN convention_collective ON convention_collective.siret = etablissement.siret
                        LEFT JOIN rge ON rge.siret = etablissement.siret
                        LEFT JOIN uai ON uai.siret = etablissement.siret
                        WHERE etablissement.siren = unite_legale.siren
                    )
                ) as etablissements,
            spectacle.est_entrepreneur_spectacle,
            spectacle.statut_entrepreneur_spectacle,
            finess_juridique.liste_finess_juridique,
            egapro.egapro_renseignee,
            bilan_ges.bilan_ges_renseigne,
            achats_responsables.est_achats_responsables,
            alim_confiance.est_alim_confiance,
            patrimoine_vivant.est_patrimoine_vivant,
            aides_minimis.aide_de_minimis_renseignee,
            aides_ademe.aide_ademe_renseignee,
            avocat.est_avocat,
            colter.colter_code_insee,
            colter.colter_code,
            colter.colter_niveau,
            ess_france.est_ess_france,
            (SELECT json_group_array(
                json_object(
                    'siren', siren,
                    'nom', nom,
                    'prenom', prenom,
                    'date_naissance', date_naissance,
                    'sexe', sexe,
                    'fonction', fonction
                    )
                ) FROM
                (
                    SELECT DISTINCT siren, nom, prenom, date_naissance,
                    sexe, fonction
                    FROM elus
                    WHERE siren = unite_legale.siren
                )
            ) as colter_elus,
            organisme_formation.est_qualiopi,
            organisme_formation.liste_id_organisme_formation,
            marche_inclusion.est_siae,
            marche_inclusion.type_siae,
            tva.liste_tva,
            CASE WHEN fondation.siren IS NOT NULL THEN json_object(
                'numero_rnf', fondation.numero_rnf,
                'denomination', fondation.denomination,
                'type_organisme', fondation.type_organisme,
                'date_creation', fondation.date_creation,
                'siren', fondation.siren,
                'siret', fondation.siret,
                'adresse', fondation.adresse,
                'code_postal', fondation.code_postal,
                'ville', fondation.ville
            ) END as fondation,
            (
                SELECT json_object(
                    'date_immatriculation', date_immatriculation,
                    'date_radiation', date_radiation,
                    'indicateur_associe_unique', indicateur_associe_unique,
                    'capital_social', capital_social,
                    'date_cloture_exercice', date_cloture_exercice,
                    'duree_personne_morale', duree_personne_morale,
                    'date_fin_existence', date_fin_existence,
                    'nature_entreprise', nature_entreprise,
                    'date_debut_activite', date_debut_activite,
                    'capital_variable', capital_variable,
                    'devise_capital', devise_capital
                )
                FROM
                (
                    SELECT date_immatriculation, date_radiation,
                    indicateur_associe_unique, capital_social,
                    date_cloture_exercice, duree_personne_morale, date_fin_existence, nature_entreprise,
                    date_debut_activite, capital_variable, devise_capital
                    FROM immatriculation
                    WHERE siren = unite_legale.siren
                )
            ) as immatriculation,
            json_object(
                'radiation',
                    CASE WHEN bodacc_radiations.siren IS NOT NULL AND bodacc_radiations.visibility THEN json_object(
                        'est_radie', bodacc_radiations.est_radie,
                        'motif', bodacc_radiations.motif,
                        'id_annonce', bodacc_radiations.id_annonce,
                        'date', bodacc_radiations.date
                    ) END,
                'procedure_collective',
                    CASE WHEN bodacc_procedures_collectives.siren IS NOT NULL THEN json_object(
                        'statut', bodacc_procedures_collectives.statut,
                        'id_annonce', bodacc_procedures_collectives.id_annonce,
                        'date', bodacc_procedures_collectives.date
                    ) END
            ) as bodacc
            FROM
                unite_legale
            LEFT JOIN count_etablissement ON count_etablissement.siren = unite_legale.siren
            LEFT JOIN count_etablissement_ouvert ON count_etablissement_ouvert.siren = unite_legale.siren
            LEFT JOIN bilan_financier ON bilan_financier.siren = unite_legale.siren
            LEFT JOIN spectacle ON spectacle.siren = unite_legale.siren
            LEFT JOIN finess_juridique ON finess_juridique.siren = unite_legale.siren
            LEFT JOIN egapro ON egapro.siren = unite_legale.siren
            LEFT JOIN bilan_ges ON bilan_ges.siren = unite_legale.siren
            LEFT JOIN achats_responsables ON achats_responsables.siren = unite_legale.siren
            LEFT JOIN alim_confiance ON alim_confiance.siren = unite_legale.siren
            LEFT JOIN patrimoine_vivant ON patrimoine_vivant.siren = unite_legale.siren
            LEFT JOIN aides_minimis ON aides_minimis.siren = unite_legale.siren
            LEFT JOIN aides_ademe ON aides_ademe.siren = unite_legale.siren
            LEFT JOIN avocat ON avocat.siren = unite_legale.siren
            LEFT JOIN colter ON colter.siren = unite_legale.siren
            LEFT JOIN ess_france ON ess_france.siren = unite_legale.siren
            LEFT JOIN organisme_formation ON organisme_formation.siren = unite_legale.siren
            LEFT JOIN marche_inclusion ON marche_inclusion.siren = unite_legale.siren
            LEFT JOIN tva ON tva.siren = unite_legale.siren
            LEFT JOIN fondation ON fondation.siren = unite_legale.siren
            LEFT JOIN bodacc_radiations ON bodacc_radiations.siren = unite_legale.siren
            LEFT JOIN bodacc_procedures_collectives ON bodacc_procedures_collectives.siren = unite_legale.siren
            LEFT JOIN doublons ON doublons.siren_doublon = unite_legale.siren
            LEFT JOIN etablissement AS siege ON siege.siren = unite_legale.siren AND siege.est_siege = 'true'
            WHERE unite_legale.siren IS NOT NULL
    """


def select_fields_to_index_query(
    siren_start: str | None = None, siren_end: str | None = None
):
    """Return the query to index with filters on `unite_legale.siren`.

    Args:
        siren_start (str | None): filter every SIREN after, including this one. None means not filter applied.
        siren_end (str | None): filter every SIREN strictly before this one. None means not filter applied.
    """
    filters = []
    if siren_start is not None:
        filters.append(f"AND unite_legale.siren >= '{siren_start}'")
    if siren_end is not None:
        filters.append(f"AND unite_legale.siren < '{siren_end}'")

    return f"{SELECT_FIELDS_TO_INDEX_QUERY} {' '.join(filters)}"
