INSERT INTO FINTRACK_DB.MONITORING.PIPELINE_CHECKS (
    run_id,pipeline_name, model_name, check_type, status, severity,
    expected_value, observed_value, message
)
SELECT
    '{RUN_ID}','FinTrack', 'RAW_TRANSACTIONS', 'quality_duplicates',
    CASE WHEN nb_doublons = 0 THEN 'PASSED'
         WHEN nb_doublons <= 3 THEN 'WARNING'
         ELSE 'FAILED' END,
    CASE WHEN nb_doublons = 0 THEN 'LOW'
         WHEN nb_doublons <= 3 THEN 'MEDIUM'
         ELSE 'HIGH' END,
    '0',
    nb_doublons::VARCHAR,
    CASE WHEN nb_doublons = 0 THEN 'Aucun doublon détecté sur ID'
         ELSE nb_doublons || ' ID en doublon détectés' END
FROM (
    SELECT COUNT(*) AS nb_doublons
    FROM (
        SELECT ID, COUNT(*) AS cnt
        FROM FINTRACK_DB.RAW.RAW_TRANSACTIONS
        GROUP BY ID
        HAVING COUNT(*) > 1
    )
);