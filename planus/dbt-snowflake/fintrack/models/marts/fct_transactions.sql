    SELECT
        transaction_id,
        compte_id,
        categorie_id,

        date_transaction,
        mois_transaction,

        montant,
        montant_signe,

        type_operation,

        nom_client,

        nom_categorie,
        groupe,

        status

    FROM {{ ref('int_transactions_enriched') }}

    WHERE status = 'validee'