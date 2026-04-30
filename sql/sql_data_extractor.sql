-- ================================================================
-- 05_extract_raw_data.sql — Production Data Extraction Query
-- ================================================================
-- PURPOSE:
--   This is the query that DataExtractor.run() executes via Python.
--   It joins Table 1 + Table 2 + Table 3 to produce raw-data.csv.
--   raw-data.csv is the input to Module 05 ETL.
--
-- WHY THIS QUERY EXISTS:
--   The database [SQL] stores data efficiently in separate tables.
--   Analytics and ML need one flat table with all columns together.
--   This "denormalisation" step is what data extraction means.
--
-- THE DATA IS INTENTIONALLY MESSY:
--   After the seed scripts ran, the database has:
--     - NULL salaries (3%)
--     - NULL performance_ratings (4%)
--     - Negative salaries (5 rows)
--     - Extreme salaries (1 row with £2.5M)
--     - years_experience = 99 (4 rows — system default for unknown)
--   Module 05 ETL will detect and fix these problems.
--   We leave them in the extraction deliberately.
--
-- CHANGE INDUSTRY BEFORE RUNNING:
--   The {{INDUSTRY}} placeholder is replaced by Python with the
--   actual industry schema name from config.py.
-- ================================================================

SELECT
    p.patient_id,
    p.first_name,
    p.last_name,
    p.email,
    p.phone,
    p.date_of_birth,
    p.gender,
    p.city,
    p.insurance_type,
    p.registered_at,
    -- These come from the sales table via the aggregated subquery below
    pat_billing.num_bills,   
    pat_billing.lifetime_billed,
    pat_billing.paid_by_patient,
    pat_billing.paid_by_insurance,
    pat_billing.recovery_rate_pct,
    pat_billing.outstanding_balance,
    pat_billing.last_claim_date,
    '{industry}'::VARCHAR AS source_schema,
    CURRENT_DATE          AS extracted_date
FROM {industry}.patients AS p
LEFT JOIN (
    SELECT
        patient_id,
        COUNT(*) AS num_bills,
    	ROUND(SUM(amount_charged)::NUMERIC, 2) AS lifetime_billed,
    	ROUND(SUM(patient_paid)::NUMERIC, 2) AS paid_by_patient,
    	ROUND(SUM(insurance_paid)::NUMERIC, 2) AS paid_by_insurance,
    	ROUND(
        	(SUM(patient_paid) + SUM(insurance_paid)) * 100.0 / NULLIF(SUM(amount_charged), 0),
        	1
    	) AS recovery_rate_pct,
    	ROUND(
        SUM(amount_charged - patient_paid - insurance_paid)::NUMERIC, 2
    	) AS outstanding_balance,
    	MAX(bill_date) AS last_claim_date
    FROM healthcare.billing
    GROUP BY patient_id
) AS pat_billing
    ON p.patient_id = pat_billing.patient_id
ORDER BY outstanding_balance DESC NULLS LAST;
