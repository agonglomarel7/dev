SELECT
    id AS compte_id,
    nom_client,
    email,
    type_compte,
    date_ouverture,
    solde_initial,
    LOWER(statut) AS statut
FROM {{ source('fintrack_raw', 'RAW_COMPTES') }}