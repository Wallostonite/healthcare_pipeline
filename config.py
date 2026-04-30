# ================================================================
# This file reads settings from the .env file.
# It never contains passwords or connection strings directly.
# ================================================================

import os, pathlib, logging
from dotenv import load_dotenv

# Load all variables from .env into the environment
load_dotenv()

INDUSTRY       = os.getenv("INDUSTRY",       "healthcare")
LEARNER_SCHEMA = os.getenv("LEARNER_SCHEMA", "learner_05")

# Use .cwd() because __file__ is only for .py scripts
PROJECT_ROOT = pathlib.Path(os.getcwd()).resolve()
SQL_DIR = PROJECT_ROOT / "sql"
DATA_DIR = PROJECT_ROOT / "data" / "raw-data.csv"

# Optional: Ensure the directories exist so the tests don't fail
SQL_DIR.mkdir(exist_ok=True)
(PROJECT_ROOT / "data").mkdir(exist_ok=True)

print(f"Paths set! SQL_DIR is: {SQL_DIR}")

RAW_DATA_PATH = DATA_DIR / "raw-data.csv"

# DB_URL comes entirely from .env — no fallback with credentials here.
# If .env is missing or DB_URL is not set, DB_AVAILABLE will be False
# and the project will tell you clearly what to do.
DB_URL = os.getenv("DB_URL", "")

try:
    from sqlalchemy import create_engine
    if not DB_URL:
        raise ValueError("DB_URL not set. Check your .env file.")
    engine = create_engine(DB_URL, pool_pre_ping=True,
                           connect_args={"connect_timeout": 10})
    with engine.connect() as c:
        c.execute(__import__("sqlalchemy").text("SELECT 1"))
    DB_AVAILABLE = True
except Exception as e:
    engine       = None
    DB_AVAILABLE = False

def _setup_logger():
    lgr = logging.getLogger("module03")
    lgr.setLevel(logging.INFO)
    if not lgr.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))
        lgr.addHandler(h)
    return lgr

logger = _setup_logger()

if not DB_AVAILABLE:
    logger.warning("Database not connected. Check your .env file — DB_URL must be set.")
