INSERT INTO FINTRACK_DB.MONITORING.PIPELINE_CHECKS (
     run_id,pipeline_name, model_name, check_type, status, severity,
    expected_value, observed_value, message
)
SELECT
    '{RUN_ID}','FinTrack', 'RAW_TRANSACTIONS', 'volume',
    CASE WHEN COUNT(*) < 10000 THEN 'FAILED'
         WHEN COUNT(*) < 20000 THEN 'WARNING'
         ELSE 'PASSED' END,
    CASE WHEN COUNT(*) < 10000 THEN 'CRITICAL'
         WHEN COUNT(*) < 20000 THEN 'MEDIUM'
         ELSE 'LOW' END,
    '>= 20000',
    COUNT(*)::VARCHAR,
    CASE WHEN COUNT(*) < 10000 THEN 'Volume de transactions anormalement faible'
         WHEN COUNT(*) < 20000 THEN 'Volume de transactions inférieur au seuil attendu'
         ELSE 'Volume de transactions normal' END
FROM FINTRACK_DB.RAW.RAW_TRANSACTIONS;