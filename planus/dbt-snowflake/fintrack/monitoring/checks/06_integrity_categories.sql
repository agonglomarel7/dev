INSERT INTO FINTRACK_DB.MONITORING.PIPELINE_CHECKS (
    run_id,pipeline_name, model_name, check_type, status, severity,
    expected_value, observed_value, message
)
SELECT
    '{RUN_ID}','FinTrack', 'RAW_TRANSACTIONS', 'integrity_categories',
    CASE WHEN nb_orphelins = 0 THEN 'PASSED'
         WHEN nb_orphelins <= 5 THEN 'WARNING'
         ELSE 'FAILED' END,
    CASE WHEN nb_orphelins = 0 THEN 'LOW'
         WHEN nb_orphelins <= 5 THEN 'MEDIUM'
         ELSE 'CRITICAL' END,
    '0',
    nb_orphelins::VARCHAR,
    CASE WHEN nb_orphelins = 0 THEN 'Toutes les transactions ont une catégorie valide'
         ELSE nb_orphelins || ' transactions référencent une CATEGORIE_ID inexistante' END
FROM (
    SELECT COUNT(*) AS nb_orphelins
    FROM FINTRACK_DB.RAW.RAW_TRANSACTIONS t
    LEFT JOIN FINTRACK_DB.RAW.RAW_CATEGORIES cat ON t.CATEGORIE_ID = cat.ID
    WHERE cat.ID IS NULL
);