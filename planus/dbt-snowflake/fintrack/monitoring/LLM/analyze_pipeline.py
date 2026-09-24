import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))


from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))  # remonte à la racine

print("Clé chargée :", os.environ.get("ANTHROPIC_API_KEY", "❌ NON TROUVÉE")[:15] + "...")

from test_connection import get_snowflake_connection
import anthropic
import json

def fetch_latest_checks(conn):
    """Récupère l'état actuel du pipeline depuis la vue de synthèse"""
    cur = conn.cursor()
    cur.execute("""
        SELECT check_type, model_name, status, severity,
               expected_value, observed_value, message, check_timestamp
        FROM FINTRACK_DB.MONITORING.VW_LATEST_CHECKS
        ORDER BY
            CASE severity
                WHEN 'CRITICAL' THEN 1
                WHEN 'HIGH' THEN 2
                WHEN 'MEDIUM' THEN 3
                ELSE 4
            END
    """)
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    cur.close()
    return [dict(zip(columns, row)) for row in rows]

def build_prompt(checks):
    """Transforme les résultats en prompt structuré pour le LLM"""
    checks_json = json.dumps(checks, indent=2, default=str)
    return f"""Tu es un data analyst qui supervise un pipeline de données financières (FinTrack).

Voici l'état actuel de tous les contrôles de qualité du pipeline (le dernier résultat de chaque contrôle) :

{checks_json}

Analyse ces résultats et réponds de façon structurée :
1. Résumé global de l'état du pipeline (en 1-2 phrases)
2. Liste des problèmes détectés, classés par priorité (CRITICAL d'abord)
3. Pour chaque problème critique ou failed, une hypothèse probable de cause
4. Recommandation d'action immédiate si nécessaire

Sois concis et orienté action, pas de blabla générique."""

def analyze_with_llm(checks):
    client = anthropic.Anthropic()  # nécessite ANTHROPIC_API_KEY en variable d'environnement
    prompt = build_prompt(checks)

    response = client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=1500,
        messages=[{"role": "user", "content": prompt}]
    )
    return response.content[0].text

if __name__ == '__main__':
    conn = get_snowflake_connection()
    checks = fetch_latest_checks(conn)
    conn.close()

    print(f"📊 {len(checks)} contrôles récupérés\n")

    print("=== Analyse LLM ===\n")
    analysis = analyze_with_llm(checks)
    print(analysis)