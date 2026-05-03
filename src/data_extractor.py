# ================================================================
# src/data_extractor.py
# ================================================================
# CONTEXT:
#   SQLQueryRunner can run any query. DataExtractor runs ONE specific
#   query: the production extraction query in 05_extract_raw_data.sql.
#
#   DataExtractor is the DELIVERY of Module 03.
#   Its output — raw-data.csv — is the INPUT to Module 05 ETL.
#
# THE PIPELINE CONNECTION:
#   Module 03 DataExtractor → raw-data.csv → Module 05 ETLPipeline
# ================================================================

import sys, pathlib

_root = pathlib.Path(__file__).resolve().parent
while not (_root / "config.py").exists() and _root != _root.parent:
    _root = _root.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

import pandas as pd
from config import INDUSTRY, RAW_DATA_PATH, DB_AVAILABLE, logger
from src.query_runner import SQLQueryRunner


class DataExtractor:
    """
    Runs the production extraction query and saves raw-data.csv.

    This class has one job: extract the data and save it.
    SQLQueryRunner handles connection and execution.
    DataExtractor handles the business logic of which query to run.
    """

    def __init__(self):
        self.industry  = INDUSTRY
        self.runner    = SQLQueryRunner()
        self.raw_df    = None    # populated by extract()
        self._status   = "ready"

    def extract(self) -> "DataExtractor":
        """
        Run 05_extract_raw_data.sql and load results into self.raw_df.

        If the database is unavailable, generates synthetic data
        so Module 05 can still be demonstrated offline.
        """
        logger.info(f"[EXTRACT] Starting extraction — industry: {self.industry}")

        if DB_AVAILABLE:
            self.raw_df = self.runner.run_file("sql_data_extractor.sql")
        else:
            logger.warning("[EXTRACT] DB unavailable — generating synthetic raw data")
            self.raw_df = self._synthetic_raw_data()

        if self.raw_df is None or len(self.raw_df) == 0:
            logger.warning("[EXTRACT] Query returned 0 rows — using synthetic data")
            self.raw_df = self._synthetic_raw_data()

        self._status = "extracted"
        logger.info(f"[EXTRACT] {len(self.raw_df):,} rows × {self.raw_df.shape[1]} columns extracted")
        return self

    def save(self) -> "DataExtractor":
        """
        Save self.raw_df to raw-data.csv.
        This file is the input to Module 05 ETL.
        """
        if self.raw_df is None or len(self.raw_df) == 0:
            logger.error("[EXTRACT] No data to save. Run extract() first.")
            return self

        self.raw_df.to_csv(RAW_DATA_PATH, index=False, encoding="utf-8")
        file_size_kb = RAW_DATA_PATH.stat().st_size / 1024
        logger.info(f"[EXTRACT] Saved {len(self.raw_df):,} rows to {RAW_DATA_PATH.name} ({file_size_kb:.1f} KB)")
        self._status = "saved"
        return self

    def report(self) -> None:
        """Print a summary of the extraction results."""
        if self.raw_df is None:
            print("No data extracted. Run extract() first.")
            return

        print()
        print("=" * 60)
        print(f"  MODULE 03 — EXTRACTION COMPLETE | {self.industry.upper()}")
        print("=" * 60)
        print(f"  Rows extracted:    {len(self.raw_df):,}")
        print(f"  Columns:           {self.raw_df.shape[1]}")
        print(f"  Output file:       {RAW_DATA_PATH.name}")
        print(f"  File size:         {RAW_DATA_PATH.stat().st_size/1024:.1f} KB" if RAW_DATA_PATH.exists() else "")
        print()
        print(f"  DATA QUALITY ISSUES IN RAW DATA (intentional — Module 05 will fix):")
        nulls = self.raw_df.isna().sum()
        for col in nulls[nulls > 0].index:
            pct = round(nulls[col] / len(self.raw_df) * 100, 1)
            print(f"    NULL {col}: {nulls[col]:,} rows ({pct}%)")

        if "amount_charged" in self.raw_df.columns:
            neg_amt = (self.raw_df["amount_charged"] < 0).sum()
            if neg_amt:
                print(f"    Negative charges: {neg_amt} rows")

        print()
        print(f"  NEXT STEP: Copy raw-data.csv to Module 05 and run:")
        print(f"    python module-05-data-engineering-and-etl/run.py")
        print("=" * 60)

    @staticmethod
    def _synthetic_raw_data(n: int = 300) -> pd.DataFrame:
        """
        Generate synthetic HEALTHCARE data (patient + billing) matching the extraction query output.
        Includes intentional data quality issues for Module 05 ETL practice.
        """
        import random, datetime
        import numpy as np
        
        # 🔒 Deterministic seeding for reproducible tests
        random.seed(42)
        np.random.seed(42)
        
        # Healthcare sample data pools
        first_names = ["Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy", "Ken", "Lily"]
        last_names  = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Wilson", "Taylor"]
        genders     = ["M", "F", "Other", None]  # None = missing
        blood_types = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-", None]
        insurances  = ["Medicare", "Medicaid", "BlueCross", "Aetna", "Cigna", "Self-Pay", "None", None]
        cities      = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose", None]
        payment_methods = ["Credit Card", "Debit Card", "Bank Transfer", "Cash", "Check", "Insurance Direct", None]
        payment_statuses  = ["paid", "pending", "denied", "partial", None]
        
        today = datetime.date.today()
        rows = []
        
        for i in range(1, n + 1):
            # --- Patient demographics ---
            age_days = random.randint(0 * 365, 95 * 365)  # ages 0–95
            dob = today - datetime.timedelta(days=age_days)
            registered = today - datetime.timedelta(days=random.randint(0, 365*3))
            
            # --- Billing fields ---
            amount = round(random.uniform(50, 5000), 2) if random.random() > 0.05 else None
            if amount and random.random() < 0.02:  # ~2% negative charges (data error)
                amount = -abs(amount)
            
            insurance_paid = round(amount * random.uniform(0, 1), 2) if amount and random.random() > 0.1 else None
            patient_paid = round(amount - insurance_paid, 2) if amount and insurance_paid else None
            
            # Intentional sentinel value: 99999.99 = placeholder for missing amount
            if random.random() < 0.03 and amount is None:
                amount = 99999.99
            
            rows.append({
                # ===== PATIENT TABLE FIELDS =====
                "patient_id":      i,
                "first_name":      random.choice(first_names),
                "last_name":       random.choice(last_names),
                "date_of_birth":   dob.isoformat(),
                "gender":          random.choice(genders),
                "blood_type":      random.choice(blood_types),
                "email":           f"patient{i}@healthmail.com" if random.random() > 0.08 else None,
                "phone":           f"+1-555-{random.randint(100,999)}-{random.randint(1000,9999)}" if random.random() > 0.1 else None,
                "city":            random.choice(cities),
                "insurance_type":  random.choice(insurances),
                "registered_at":   registered.isoformat(),
                
                # ===== BILLING TABLE FIELDS =====
                "bill_id":         f"BILL-{i:06d}",
                "appointment_id":  f"APT-{random.randint(10000, 99999)}",
                "amount_charged":  amount,
                "insurance_paid":  insurance_paid,
                "patient_paid":    patient_paid,
                "payment_status":  random.choice(payment_statuses),
                "bill_date":       (today - datetime.timedelta(days=random.randint(0, 180))).isoformat(),
                "payment_method":  random.choice(payment_methods),
                
                # ===== PIPELINE METADATA =====
                "source_schema":   "healthcare_pipeline",
                "extracted_date":  today.isoformat(),
            })
        
        df = pd.DataFrame(rows)
        
        # 🔪 Inject additional intentional data quality issues
        # ~10% NULL insurance_type
        null_mask = np.random.random(n) < 0.10
        df.loc[null_mask, "insurance_type"] = None
        
        # ~8% NULL payment_status
        null_mask = np.random.random(n) < 0.08
        df.loc[null_mask, "payment_status"] = None
        
        # ~5% mismatched totals (patient_paid + insurance_paid != amount_charged)
        mismatch_idx = np.random.choice(n, size=int(n*0.05), replace=False)
        for idx in mismatch_idx:
            if df.loc[idx, "amount_charged"] and df.loc[idx, "insurance_paid"]:
                df.loc[idx, "patient_paid"] = df.loc[idx, "amount_charged"] + random.uniform(10, 100)  # intentional mismatch
        
        return df

    def __str__(self):
        return f"DataExtractor(industry={self.industry!r}, status={self._status!r})"

    def __repr__(self):
        return f"DataExtractor(industry={self.industry!r})"