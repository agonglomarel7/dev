import yaml
import os
import snowflake.connector

def get_snowflake_connection():
    profiles_path = os.path.expanduser('~/.dbt/profiles.yml')
    with open(profiles_path) as f:
        profiles = yaml.safe_load(f)

    target_profile = profiles['fintrack']
    target_name = target_profile['target']
    creds = target_profile['outputs'][target_name]

    return snowflake.connector.connect(
        account=creds['account'],
        user=creds['user'],
        password=creds.get('password'),
        role=creds.get('role'),
        warehouse=creds.get('warehouse'),
        database=creds.get('database'),
        schema='MONITORING',
        disable_ocsp_checks=True
    )

if __name__ == '__main__':
    conn = get_snowflake_connection()
    cur = conn.cursor()
    cur.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
    result = cur.fetchone()
    print("✅ Connexion réussie")
    print(f"User: {result[0]}, Role: {result[1]}, Warehouse: {result[2]}, Database: {result[3]}, Schema: {result[4]}")

    cur.execute("SELECT COUNT(*) FROM FINTRACK_DB.MONITORING.PIPELINE_CHECKS")
    count = cur.fetchone()[0]
    print(f"📊 PIPELINE_CHECKS contient déjà {count} lignes")

    cur.close()
    conn.close()