SELECT
    ID AS transaction_id,
    COMPTE_ID AS compte_id,
    CAST(DATE_TRANSACTION AS TIMESTAMP_NTZ) AS date_transaction,
    MONTANT AS montant,
    TYPE_OPERATION AS type_operation,
    CATEGORIE_ID AS categorie_id,
    "DESCRIPTION" AS description,
    STATUT AS status,

    CASE
        WHEN TYPE_OPERATION = 'credit' THEN MONTANT
        WHEN TYPE_OPERATION = 'debit' THEN -MONTANT
        ELSE MONTANT
    END AS montant_signe,

    _LOADED_AT AS loaded_at

FROM {{ source('fintrack_raw', 'RAW_TRANSACTIONS') }}