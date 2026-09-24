INSERT INTO FINTRACK_DB.MONITORING.PIPELINE_CHECKS (
    run_id,pipeline_name, model_name, check_type, status, severity,
    expected_value, observed_value, message
)
SELECT
    '{RUN_ID}','FinTrack', 'RAW_COMPTES', 'business_solde_negatif_epargne',
    CASE WHEN nb_comptes = 0 THEN 'PASSED' ELSE 'FAILED' END,
    CASE WHEN nb_comptes = 0 THEN 'LOW' ELSE 'CRITICAL' END,
    '0',
    nb_comptes::VARCHAR,
    CASE WHEN nb_comptes = 0 THEN 'Aucun compte épargne en solde négatif'
         ELSE nb_comptes || ' comptes épargne présentent un solde négatif' END
FROM (
    SELECT COUNT(*) AS nb_comptes
    FROM FINTRACK_DB.RAW.RAW_COMPTES
    WHERE TYPE_COMPTE = 'EPARGNE' AND SOLDE_INITIAL < 0
);