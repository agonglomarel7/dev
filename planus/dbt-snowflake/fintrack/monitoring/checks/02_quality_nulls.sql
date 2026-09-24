INSERT INTO FINTRACK_DB.MONITORING.PIPELINE_CHECKS (
    run_id,pipeline_name, model_name, check_type, status, severity,
    expected_value, observed_value, message
)
SELECT
    '{RUN_ID}','FinTrack', 'RAW_TRANSACTIONS', 'quality_nulls',
    CASE WHEN nb_nulls = 0 THEN 'PASSED'
         WHEN nb_nulls <= 5 THEN 'WARNING'
         ELSE 'FAILED' END,
    CASE WHEN nb_nulls = 0 THEN 'LOW'
         WHEN nb_nulls <= 5 THEN 'MEDIUM'
         ELSE 'CRITICAL' END,
    '0',
    nb_nulls::VARCHAR,
    CASE WHEN nb_nulls = 0 THEN 'Aucune valeur NULL détectée'
         ELSE nb_nulls || ' valeurs NULL détectées sur colonnes critiques (ID, COMPTE_ID, CATEGORIE_ID)' END
FROM (
    SELECT
        COUNT_IF(ID IS NULL)
        + COUNT_IF(COMPTE_ID IS NULL)
        + COUNT_IF(CATEGORIE_ID IS NULL) AS nb_nulls
    FROM FINTRACK_DB.RAW.RAW_TRANSACTIONS
);