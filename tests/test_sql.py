# ================================================================
# tests/test_sql.py — Unit Tests for the healthcare pipeline
# ================================================================

import sys, pathlib
try:
    _root = pathlib.Path(__file__).resolve().parent.parent
except NameError:
    _root = pathlib.Path.cwd().parent

if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

import pandas as pd
import config
from src.query_runner   import SQLQueryRunner
from src.data_extractor import DataExtractor


def test_sql_files_exist():
    """All expected SQL teaching files must exist in the sql/ directory."""
    expected = ["sql_data_extractor.sql"]
    for fname in expected:
        assert (config.SQL_DIR / fname).exists(), f"SQL file missing: {fname}"


def test_sql_files_contain_select_keyword():
    """Each SQL file must contain at least one SELECT statement."""
    for fname in ["sql_data_extractor.sql"]:
        content = (config.SQL_DIR / fname).read_text()
        assert "SELECT" in content.upper(), f"No SELECT found in {fname}"


def test_extract_sql_contains_industry_placeholder():
    """sql_data_extractor.sql must use {industry} placeholder."""
    content = (config.SQL_DIR / "sql_data_extractor.sql").read_text()
    assert "{industry}" in content, "Extraction SQL must use {industry} placeholder for multi-schema support"


def test_query_runner_returns_dataframe():
    """SQLQueryRunner.run() must always return a DataFrame (never crashes)."""
    runner = SQLQueryRunner()
    df = runner.run("SELECT 1 AS test_col")
    assert isinstance(df, pd.DataFrame), "run() must always return a DataFrame"


def test_query_runner_handles_bad_sql_gracefully():
    """A broken query should return empty DataFrame, not crash."""
    runner = SQLQueryRunner()
    df = runner.run("THIS IS NOT VALID SQL AT ALL")
    assert isinstance(df, pd.DataFrame), "run() should return empty DataFrame on SQL error, not raise exception"


def test_query_runner_history_records_each_run():
    """Every query run must appear in the history log."""
    runner = SQLQueryRunner()
    initial_count = len(runner.history)
    runner.run("SELECT 1")
    runner.run("SELECT 2")
    assert len(runner.history) == initial_count + 2, "history should record each query run"


def test_extractor_synthetic_data_has_required_columns():
    """Synthetic fallback data must contain all required healthcare columns."""
    raw = DataExtractor._synthetic_raw_data(50)
    
    assert isinstance(raw, pd.DataFrame), "Expected DataFrame output"
    assert len(raw) == 50, f"Expected 50 rows, got {len(raw)}"
    
    # ✅ Full healthcare schema: patient + billing + metadata
    required = [
        # Patient table
        "patient_id", "first_name", "last_name", "date_of_birth",
        "gender", "blood_type", "email", "phone", "city",
        "insurance_type", "registered_at",
        # Billing table
        "bill_id", "appointment_id", "amount_charged",
        "insurance_paid", "patient_paid", "payment_status",
        "bill_date", "payment_method",
        # Pipeline metadata
        "source_schema", "extracted_date"
    ]
    
    for col in required:
        assert col in raw.columns, f"Synthetic data missing column: {col}"


def test_extractor_synthetic_data_has_quality_issues():
    """Synthetic data must contain intentional data quality issues."""
    raw = DataExtractor._synthetic_raw_data(300)
    
    # Should have some NULL amounts_charged
    null_amounts = raw["amount_charged"].isna().sum()
    assert null_amounts > 0, "Synthetic raw data should have some NULL amount_charged values"
    
    # Should have some sentinel 99999.99 placeholder values
    sentinel_vals = (raw["amount_charged"] == 99999.99).sum()
    assert sentinel_vals > 0, "Synthetic raw data should have some 99999.99 sentinel values"
    
    # Should have some NULL insurance_type
    null_ins = raw["insurance_type"].isna().sum()
    assert null_ins > 0, "Synthetic raw data should have some NULL insurance_type values"
    
    # Should have some NULL payment_status
    null_pay = raw["payment_status"].isna().sum()
    assert null_pay > 0, "Synthetic raw data should have some NULL payment_status values"


def test_extractor_save_creates_csv(tmp_path, monkeypatch):
    """DataExtractor.save() must create raw-data.csv at the patched path."""
    target_path = tmp_path / "raw-data.csv"
    
    # ✅ Patch the exact namespace where save() resolves RAW_DATA_PATH
    # Because data_extractor.py does: from config import RAW_DATA_PATH
    monkeypatch.setattr("src.data_extractor.RAW_DATA_PATH", target_path)
    
    extractor = DataExtractor()
    extractor.raw_df = DataExtractor._synthetic_raw_data(50)
    extractor._status = "extracted"
    extractor.save()
    
    # ✅ Assert directly on the patched path
    assert target_path.exists(), f"save() should create raw-data.csv at {target_path}"
    
    reloaded = pd.read_csv(target_path)
    assert len(reloaded) == 50, "Saved CSV should have correct row count"
    
    # Optional: verify schema preserved after save/load
    for col in ["patient_id", "bill_id", "amount_charged"]:
        assert col in reloaded.columns, f"Saved CSV missing column: {col}"

if __name__ == "__main__":
    # ✅ Safe execution via pytest (handles fixtures, teardown, and reporting)
    import pytest
    pytest.main(["-v", "--tb=short", __file__])