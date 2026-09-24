SELECT
    compte_id,
    nom_client,
    email,
    type_compte,
    date_ouverture,
    solde_initial,
    statut,

    DATEDIFF(
        'day',
        date_ouverture,
        CURRENT_DATE()
    ) AS anciennete_jours

FROM {{ ref('stg_comptes') }}