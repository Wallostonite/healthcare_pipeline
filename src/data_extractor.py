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
            self.raw_df = self.runner.run_file("05_extract_raw_data.sql")
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

        if "salary" in self.raw_df.columns:
            neg_sal = (self.raw_df["salary"] < 0).sum()
            if neg_sal:
                print(f"    Negative salaries: {neg_sal} rows")

        print()
        print(f"  NEXT STEP: Copy raw-data.csv to Module 05 and run:")
        print(f"    python module-05-data-engineering-and-etl/run.py")
        print("=" * 60)

    @staticmethod
    def _synthetic_raw_data(n: int = 300) -> pd.DataFrame:
        """Generate synthetic raw data matching the extraction query output."""
        import random, datetime
        random.seed(42)
        import numpy as np; np.random.seed(42)
        depts   = ["Engineering","Sales","Data Science","Finance","HR","Product","Operations"]
        ratings = ["Exceeds","Meets","Below", None, None]
        cities  = ["New York","San Francisco","Chicago","Austin","Seattle",None]
        rows = []
        for i in range(1, n+1):
            dept   = random.choice(depts)
            sal    = round(random.uniform(45000, 180000), -2) if random.random() > 0.03 else None
            if sal and random.random() < 0.005:
                sal = sal * -1
            rows.append({
                "employee_id":      i,
                "first_name":       random.choice(["Alice","Bob","Carol","David","Eve","Frank"]),
                "last_name":        random.choice(["Smith","Johnson","Williams","Brown","Jones"]),
                "email":            f"emp{i}@globaltech.com",
                "department":       dept,
                "salary":           sal,
                "hire_date":        f"202{random.randint(0,4)}-{random.randint(1,12):02d}-01",
                "performance_rating": random.choice(ratings),
                "years_experience": random.choice([1,2,3,5,8,10,15,20,99]),
                "city":             random.choice(cities),
                "is_active":        random.random() > 0.04,
                "num_sales":        random.randint(0,50),
                "total_revenue":    round(random.uniform(0,500000),2),
                "avg_sale_value":   round(random.uniform(500,8000),2),
                "num_completed":    random.randint(0,45),
                "source_schema":    "bootcamp_data",
                "extracted_date":   datetime.date.today().isoformat(),
            })
        return pd.DataFrame(rows)

    def __str__(self):
        return f"DataExtractor(industry={self.industry!r}, status={self._status!r})"

    def __repr__(self):
        return f"DataExtractor(industry={self.industry!r})"
