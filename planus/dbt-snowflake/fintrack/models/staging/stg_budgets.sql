SELECT 
    ID AS budget_id,
    COMPTE_ID AS compte_id,
    CATEGORIE_ID AS categorie_id,
    Cast(MOIS AS DATE) AS mois,
    MONTANT_PREVU AS montant_prevu,
    _lOADED_at AS loaded_at

FROM {{ source('fintrack_raw', 'RAW_BUDGETS') }}