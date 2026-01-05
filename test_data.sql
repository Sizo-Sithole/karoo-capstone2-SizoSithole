USE DATABASE DB1;
USE SCHEMA PUBLIC;

ALTER TABLE SUPPLIERS ADD COLUMN IF NOT EXISTS status STRING;
ALTER TABLE SUPPLIERS ADD COLUMN IF NOT EXISTS last_audit DATE;

UPDATE SUPPLIERS
SET status = 'Active', last_audit = NULL
WHERE status IS NULL OR status <> 'Active';

MERGE INTO CERTIFICATIONS t
USING (
  SELECT column1::INTEGER AS supplier_id,
         column2::STRING  AS cert_name,
         column3::DATE    AS issued_date,
         column4::DATE    AS expiry_date
  FROM VALUES
    (1, 'Organic Certified', DATEADD(day, -300, CURRENT_DATE()), DATEADD(day, 120, CURRENT_DATE())),
    (2, 'Organic Certified', DATEADD(day, -300, CURRENT_DATE()), DATEADD(day, 200, CURRENT_DATE())),
    (3, 'Organic Certified', DATEADD(day, -300, CURRENT_DATE()), DATEADD(day, 10,  CURRENT_DATE())),
    (4, 'Organic Certified', DATEADD(day, -300, CURRENT_DATE()), DATEADD(day, -5,  CURRENT_DATE())),
    (5, 'Organic Certified', DATEADD(day, -300, CURRENT_DATE()), NULL),
    (6, 'Organic Certified', DATEADD(day, -300, CURRENT_DATE()), DATEADD(day, 25,  CURRENT_DATE()))
) s
ON t.supplier_id = s.supplier_id AND t.cert_name = s.cert_name
WHEN MATCHED THEN UPDATE SET issued_date = s.issued_date, expiry_date = s.expiry_date
WHEN NOT MATCHED THEN INSERT (supplier_id, cert_name, issued_date, expiry_date)
VALUES (s.supplier_id, s.cert_name, s.issued_date, s.expiry_date);

MERGE INTO HARVEST_LOG t
USING (
  SELECT column1::INTEGER AS supplier_id,
         column2::DATE    AS harvest_date,
         column3::STRING  AS crop,
         column4::NUMBER(12,2) AS quantity_kg,
         column5::STRING  AS grade,
         column6::STRING  AS notes
  FROM VALUES
    (1, DATEADD(day, -60, CURRENT_DATE()), 'Rooibos', 1000.00, 'A', 'rolling avg baseline'),
    (1, DATEADD(day, -30, CURRENT_DATE()), 'Rooibos', 1000.00, 'A', 'rolling avg baseline'),
    (1, DATEADD(day, -5,  CURRENT_DATE()), 'Rooibos',  500.00, 'A', 'yield decline trigger'),
    (6, DATEADD(day, -55, CURRENT_DATE()), 'Dates',   1200.00, 'A', 'rolling avg baseline'),
    (6, DATEADD(day, -25, CURRENT_DATE()), 'Dates',   1100.00, 'A', 'rolling avg baseline'),
    (6, DATEADD(day, -3,  CURRENT_DATE()), 'Dates',    700.00, 'A', 'yield decline trigger')
) s
ON t.supplier_id = s.supplier_id AND t.harvest_date = s.harvest_date AND t.crop = s.crop
WHEN MATCHED THEN UPDATE SET quantity_kg = s.quantity_kg, grade = s.grade, notes = s.notes
WHEN NOT MATCHED THEN INSERT (supplier_id, harvest_date, crop, quantity_kg, grade, notes)
VALUES (s.supplier_id, s.harvest_date, s.crop, s.quantity_kg, s.grade, s.notes);

MERGE INTO ORDERSS t
USING (
  SELECT column1::INTEGER AS order_id,
         column2::INTEGER AS supplier_id,
         column3::DATE    AS order_date,
         column4::NUMBER(12,2) AS total_price
  FROM VALUES
    (9001, 1, DATEADD(day, -10, CURRENT_DATE()), 4200.00),
    (9002, 3, DATEADD(day, -20, CURRENT_DATE()), 3100.00),
    (9003, 4, DATEADD(day, -15, CURRENT_DATE()), 5600.00),
    (9004, 6, DATEADD(day, -7,  CURRENT_DATE()), 3800.00),
    (9005, 2, DATEADD(day, -120, CURRENT_DATE()), 2500.00)
) s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET supplier_id = s.supplier_id, order_date = s.order_date, total_price = s.total_price
WHEN NOT MATCHED THEN INSERT (order_id, supplier_id, order_date, total_price)
VALUES (s.order_id, s.supplier_id, s.order_date, s.total_price);
