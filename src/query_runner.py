# ================================================================
# src/query_runner.py
# ================================================================
# CONTEXT:
#   We wrote SQL in .sql files. Now we need to EXECUTE those queries
#   from Python and get back pandas DataFrames we can work with.
#
# THE ANALOGY:
#   Think of SQLQueryRunner as a translator.
#   You hand it a SQL query (a string).
#   It sends that query to PostgreSQL.
#   PostgreSQL sends back rows of data.
#   SQLQueryRunner catches those rows and packages them as a DataFrame.
#
# KEY pandas FUNCTION: pd.read_sql()
#   pd.read_sql(sql_string, engine) executes SQL and returns a DataFrame.
#   This is what powers every Python + database workflow.
#
# WHY A CLASS AND NOT JUST pd.read_sql() DIRECTLY?
#   The class adds:
#     - Error handling (what if the query fails?)
#     - Logging (track what ran and when)
#     - Timing (how long did each query take?)
#     - Query loading from .sql files
#   These extras make it production-grade rather than just a script.
# ================================================================

import sys, pathlib, time

_root = pathlib.Path(__file__).resolve().parent
while not (_root / "config.py").exists() and _root != _root.parent:
    _root = _root.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

import pandas as pd
from config import engine, DB_AVAILABLE, SQL_DIR, INDUSTRY, logger


class SQLQueryRunner:
    """
    Executes SQL queries against the Supabase PostgreSQL database.
    Returns results as pandas DataFrames.

    Attributes
    ──────────
    industry   str           the configured industry schema
    history    list[dict]    log of every query run (for auditing)
    """

    def __init__(self):
        self.industry = INDUSTRY
        self.history  = []   # audit log: each entry records a query run
        logger.info(f"SQLQueryRunner ready — db_available: {DB_AVAILABLE}")

    def run(self, sql: str, params: dict = None) -> pd.DataFrame:
        """
        Execute a SQL query and return results as a DataFrame.

        Args:
            sql     the SQL query string to execute
            params  optional dict of parameters for parameterised queries
                    e.g. params={"dept": "Engineering"}
                    used as WHERE department = :dept in the SQL

        Returns:
            pd.DataFrame with query results (empty DataFrame if query fails)
        """
        if not DB_AVAILABLE or engine is None:
            logger.warning("[SQL] Database not available. Returning empty DataFrame.")
            return pd.DataFrame()

        # Replace {industry} placeholder with the actual schema name
        # This makes SQL files reusable across different industries
        sql = sql.replace("{industry}", self.industry)

        start_time = time.time()   # record start time for performance logging

        try:
            # pd.read_sql() executes SQL and returns a DataFrame
            # params → passed as bind parameters to prevent SQL injection
            df = pd.read_sql(sql, engine, params=params)

            duration_ms = round((time.time() - start_time) * 1000, 1)

            # Record this query in the history log
            self.history.append({
                "sql_preview": sql[:80].strip(),   # first 80 chars for the log
                "rows":        len(df),
                "cols":        len(df.columns),
                "duration_ms": duration_ms,
                "status":      "success",
            })

            logger.info(
                f"[SQL] Query complete — "
                f"{len(df):,} rows × {len(df.columns)} cols | "
                f"{duration_ms}ms"
            )

            return df

        except Exception as e:
            self.history.append({
                "sql_preview": sql[:80].strip(),
                "rows":        0,
                "cols":        0,
                "duration_ms": round((time.time() - start_time) * 1000, 1),
                "status":      f"error: {str(e)[:100]}",
            })
            logger.error(f"[SQL] Query failed: {e}")
            return pd.DataFrame()   # return empty DataFrame so callers do not crash

    def run_file(self, filename: str) -> pd.DataFrame:
        """
        Load a .sql file and execute it.

        Args:
            filename   name of the SQL file in the sql/ directory
                       e.g. "01_basics.sql" or "05_extract_raw_data.sql"

        Returns:
            pd.DataFrame with results of the LAST query in the file.
        """
        sql_path = SQL_DIR / filename

        if not sql_path.exists():
            logger.error(f"[SQL] File not found: {sql_path}")
            return pd.DataFrame()

        logger.info(f"[SQL] Loading: {filename}")

        # Read the .sql file as a Python string
        sql_text = sql_path.read_text(encoding="utf-8")

        # Run the SQL (may contain multiple statements separated by semicolons)
        # We run the whole file and return the result of the last statement
        return self.run(sql_text)

    def demo_basics(self) -> None:
        """
        Run selected demonstration queries from 01_basics.sql.
        Used during the teaching session to show SQL concepts live.
        """
        demos = [
            ("DISTINCT insurance_type",
             f"SELECT DISTINCT insurance_type FROM {self.industry}.patients ORDER BY insurance_type"),

            ("Patients types",
             f"SELECT first_name, last_name, insurance_type, blood_group FROM {self.industry}.patients WHERE blood_group IN ('O-', 'B-') ORDER BY blood_group DESC"),

            ("Active employees in Engineering or Data Science",
             f"SELECT first_name, insurance_type FROM {self.industry}.patients WHERE insurance_type IN ('Private', 'Corporate')")
        ]


        for title, sql in demos:
            print(f"\n── {title}:")
            df = self.run(sql)
            if not df.empty:
                print(df.to_string(index=False))

    def demo_aggregation(self) -> None:
        """Demo groupby queries from 02_aggregation.sql."""
        sql = f"""
            SELECT
                patient_id,
                COUNT(*) AS num_bills,
                ROUND(SUM(amount_charged)::NUMERIC, 2) AS lifetime_billed,
                ROUND(SUM(patient_paid)::NUMERIC, 2) AS paid_by_patient,
                ROUND(SUM(insurance_paid)::NUMERIC, 2) AS paid_by_insurance
            FROM {self.industry}.billing
            GROUP BY patient_id
            ORDER BY patient_id;
        """
        print("\n── Patient Billing Summary:")  # Updated label
        df = self.run(sql)
        if not df.empty:
            print(df.to_string(index=False))


    def demo_joins(self) -> None:
        """Demo join query from 03_joins.sql."""
        sql = f"""
            SELECT
                p.first_name, 
                p.last_name, 
                p.insurance_type,
                COUNT(b.bill_id) AS num_bills,
                ROUND(COALESCE(SUM(b.amount_charged), 0)::NUMERIC, 2) AS total_billed
            FROM {self.industry}.patients AS p
            LEFT JOIN {self.industry}.billing AS b ON p.patient_id = b.patient_id
            GROUP BY p.patient_id, p.first_name, p.last_name, p.insurance_type
            ORDER BY total_billed DESC
            LIMIT 10
        """
        print("\n── Top 10 Patients by Total Billed Amount:")
        df = self.run(sql)
        if not df.empty:
            print(df.to_string(index=False))

    def __str__(self) -> str:
        return (f"SQLQueryRunner(industry={self.industry!r}, "
                f"queries_run={len(self.history)})")

    def __repr__(self) -> str:
        return f"SQLQueryRunner(industry={self.industry!r})"
