INSERT INTO FINTRACK_DB.MONITORING.PIPELINE_CHECKS (
    run_id,pipeline_name, model_name, check_type, status, severity,
    expected_value, observed_value, message
)
SELECT
    '{RUN_ID}','FinTrack', 'RAW_TRANSACTIONS', 'quality_invalid_values',
    CASE WHEN nb_invalides = 0 THEN 'PASSED'
         WHEN nb_invalides <= 5 THEN 'WARNING'
         ELSE 'FAILED' END,
    CASE WHEN nb_invalides = 0 THEN 'LOW'
         WHEN nb_invalides <= 5 THEN 'MEDIUM'
         ELSE 'HIGH' END,
    '0',
    nb_invalides::VARCHAR,
    CASE WHEN nb_invalides = 0 THEN 'Aucune valeur invalide détectée'
         ELSE nb_invalides || ' transactions avec montant nul ou date future' END
FROM (
    SELECT
        COUNT_IF(MONTANT = 0)
        + COUNT_IF(DATE_TRANSACTION > CURRENT_DATE()) AS nb_invalides
    FROM FINTRACK_DB.RAW.RAW_TRANSACTIONS
);