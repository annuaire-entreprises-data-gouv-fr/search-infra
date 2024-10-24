GET_DEMARCHE_QUERY = """
    query getDemarche(
        $demarcheNumber: Int!,
        $after: String
        $includeChamps: Boolean = true
        $includeTraitements: Boolean = true
    ) {
        demarche(number: $demarcheNumber) {
            id
            number
            title
            state
            dateCreation
            dossiers(after: $after, first: 100) {
                pageInfo {
                    hasNextPage
                    hasPreviousPage
                    startCursor
                    endCursor
                }
                nodes {
                    id
                    number
                    archived
                    state
                    dateDerniereModification
                    dateDepot
                    datePassageEnConstruction
                    datePassageEnInstruction
                    dateTraitement
                    dateExpiration
                    dateSuppressionParUsager
                    usager {
                        email
                    }
                    demandeur {
                        ... on PersonneMorale {
                            siret
                            siegeSocial
                            entreprise {
                                siren
                                formeJuridique
                                formeJuridiqueCode
                                nomCommercial
                                raisonSociale
                                nom
                                prenom
                            }
                        }
                    }
                    traitements @include(if: $includeTraitements) {
                        state
                        emailAgentTraitant
                        dateTraitement
                        motivation
                    }
                    champs @include(if: $includeChamps) {
                        id
                        champDescriptorId
                        __typename
                        label
                        stringValue
                        updatedAt
                        prefilled
                    }
                }
            }
        }
    }
"""

ARCHIVE_DOSSIER_MUTATION = """
    mutation dossierArchiver($input: DossierArchiverInput!) {
        dossierArchiver(input: $input) {
            clientMutationId
            dossier {
                id
            }
            errors {
                message
            }
        }
    }
"""
