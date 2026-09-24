import glob
import json
import os
import uuid
from datetime import datetime
from pathlib import Path

from test_connection import get_snowflake_connection


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CHECKS_DIR = os.path.join(SCRIPT_DIR, "checks")
REPORT_PATH = os.path.join(SCRIPT_DIR, "quality_report.json")

run_id = str(uuid.uuid4())

def run_sql_checks(conn):
    cur = conn.cursor()

    sql_files = sorted(
        glob.glob(os.path.join(CHECKS_DIR, "*.sql"))
    )

    print(f"📂 Recherche dans : {CHECKS_DIR}")

    if not sql_files:
        print("⚠️ Aucun fichier SQL trouvé")
        return []


    for filepath in sql_files:
        filename = os.path.basename(filepath)

        print(f"▶ Exécution de {filename}")

        with open(filepath, encoding="utf-8-sig") as file:
            sql = file.read()

        sql = sql.replace("{RUN_ID}", run_id)

        cur.execute(sql)
        print("   ✅ Check exécuté")

    # Lecture des résultats produits par les checks
    print("\n📊 Lecture des résultats...")

    cur.execute("""
        SELECT
            PIPELINE_NAME,
            MODEL_NAME,
            CHECK_TYPE,
            STATUS,
            SEVERITY,
            EXPECTED_VALUE,
            OBSERVED_VALUE,
            MESSAGE
        FROM FINTRACK_DB.MONITORING.PIPELINE_CHECKS
        WHERE PIPELINE_NAME = 'FinTrack'
        AND RUN_ID = %s
        ORDER BY CHECK_TYPE
    """, (run_id,))

    columns = [column[0].lower() for column in cur.description]
    rows = cur.fetchall()

    results = [
        dict(zip(columns, row))
        for row in rows
    ]

    cur.close()

    return results


def generate_report(results):
    if any(result["status"] == "FAILED" for result in results):
        global_status = "FAILED"
    elif any(result["status"] == "WARNING" for result in results):
        global_status = "WARNING"
    else:
        global_status = "PASSED"

    report = {
        "pipeline_name": "FinTrack",
        "generated_at": datetime.now().isoformat(),
        "status": global_status,
        "checks_count": len(results),
        "checks": results,
    }

    with open(REPORT_PATH, "w", encoding="utf-8") as file:
        json.dump(
            report,
            file,
            indent=4,
            ensure_ascii=False,
            default=str,
        )

    return report


if __name__ == "__main__":
    conn = get_snowflake_connection()

    try:
        results = run_sql_checks(conn)
        conn.commit()

        report = generate_report(results)

        print("\n📊 Résumé global")
        print("=" * 50)
        print(f"Pipeline : {report['pipeline_name']}")
        print(f"Statut   : {report['status']}")
        print(f"Checks   : {report['checks_count']}")

        print("\nDétail des contrôles :")

        for result in results:
            print(
                f"- {result['check_type']} | "
                f"{result['status']} | "
                f"{result['severity']} | "
                f"{result['observed_value']} | "
                f"{result['message']}"
            )

        print(f"\n📄 Rapport généré : {REPORT_PATH}")

    except Exception as error:
        conn.rollback()
        print(f"\n❌ Erreur : {error}")
        raise

    finally:
        conn.close()