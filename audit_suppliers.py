import os
import snowflake.connector

RISK_QUERY = """
SELECT
  supplier_id
FROM v_supplier_health
WHERE
  (next_cert_expiry IS NOT NULL AND next_cert_expiry <= DATEADD(day, 30, CURRENT_DATE()))
  OR (orders_90d = 0)
  OR (latest_yield IS NOT NULL AND rolling_avg_yield IS NOT NULL AND latest_yield < (0.8 * rolling_avg_yield))
ORDER BY supplier_id;
"""

UPDATE_SQL = """
UPDATE SUPPLIERS
SET status = %s,
    last_audit = CURRENT_DATE()
WHERE supplier_id = %s;
"""

def connect():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        database=os.getenv("SNOWFLAKE_DATABASE", "DB1"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
    )

def main():
    conn = None
    cur = None
    try:
        conn = connect()
        conn.autocommit(False)
        cur = conn.cursor()

        cur.execute("USE DATABASE DB1")
        cur.execute("USE SCHEMA PUBLIC")

        cur.execute(RISK_QUERY)
        rows = cur.fetchall()
        at_risk_ids = [r[0] for r in rows]

        if not at_risk_ids:
            conn.commit()
            print("0 suppliers require review")
            return

        params = [("Review", int(sid)) for sid in at_risk_ids]
        cur.executemany(UPDATE_SQL, params)
        conn.commit()
        print(f"{len(at_risk_ids)} suppliers require review")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Audit failed: {e}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
