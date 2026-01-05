# Karoo Capstone Phase 2: Supplier Risk & Compliance Auditor

## Files
- auditor_views.sql
- test_data.sql
- audit_suppliers.py
- README.md

## Setup
1) Run `schema.sql` from Phase 1 to ensure tables exist.
2) Run `test_data.sql` to add sample certification, harvest, and order data for monitoring.
3) Run `auditor_views.sql` to create `v_supplier_health` and validate the risk query output.
4) Run `audit_suppliers.py` to automatically set `SUPPLIERS.status = 'Review'` for flagged suppliers.

## Monitoring View Logic (v_supplier_health)
The view produces one row per supplier and calculates:
- `cert_status`:
  - `Unknown` if no expiry date exists
  - `Expired` if expiry date < current date
  - `Expiring Soon` if expiry date ≤ current date + 30 days
  - `Valid` otherwise
- `orders_90d`: number of orders with `order_date` in the last 90 days
- `latest_yield`: most recent `HARVEST_LOG.quantity_kg` per supplier
- `rolling_avg_yield`: 3-harvest rolling average using a window function and selecting the latest row

## Risk-Flagging Rules
A supplier is flagged if any of the following is true:
1) Certification expires within 30 days (including already expired)
2) Zero orders in the last 90 days
3) Latest yield is below 80% of the 3-harvest rolling average

## Compliance and Audit Considerations
- Updating supplier status creates a clear operational signal for follow-up and documentation.
- `last_audit` is stored to support audit trails and reporting on when suppliers were last checked.
- The script runs in a transaction and rolls back on error to avoid partial updates.

## Environment variables (Snowflake)
- SNOWFLAKE_ACCOUNT
- SNOWFLAKE_USER
- SNOWFLAKE_PASSWORD
- SNOWFLAKE_WAREHOUSE
- SNOWFLAKE_ROLE (optional if your account has a default role)
- SNOWFLAKE_DATABASE (defaults to DB1)
- SNOWFLAKE_SCHEMA (defaults to PUBLIC)

## Run
pip install snowflake-connector-python
python audit_suppliers.py
