"""
ETL extraction and data quality validation script.

This module:
- Connects to a MySQL database
- Runs basic data quality checks on fact tables
- Extracts dimension, fact, and optional KPI views
- Exports results to CSV and Parquet formats

Designed for analytics and portfolio use.
"""


# =============================================================================

from dataclasses import dataclass
from typing import Dict
import os
import pathlib
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

# =============================================================================

# -----------------------------
# Database configuration model
# -----------------------------

@dataclass
class DBConfig:
    """
    Holds database connection parameters.

    Attributes:
        host: Database host name or IP address
        port: Database port number
        user: Database user name
        password: Database user password
        db: Target database name
    """

    host: str
    port: int
    user: str
    password: str
    db: str

# =============================================================================

def get_engine(cfg: DBConfig) -> Engine:
    """
    Create and return a SQLAlchemy engine using MySQL and PyMySQL.

    Args:
        cfg: DBConfig object with connection parameters

    Returns:
        SQLAlchemy Engine instance
    """

    url = f"mysql+pymysql://{cfg.user}:{cfg.password}@{cfg.host}:{cfg.port}/{cfg.db}"
    return create_engine(url, pool_pre_ping=True)

# =============================================================================

def run_checks(engine: Engine) -> Dict[str, int]:
    """
    Run basic data quality checks against fact tables.

    Checks include:
    - Invalid order totals
    - Orphaned order items
    - Negative monetary values
    - Invalid line item totals

    Args:
        engine: SQLAlchemy engine

    Returns:
        Dictionary mapping check name to row count
    """

    checks = {
        "bad_total_rows": """
            SELECT COUNT(*) AS n
            FROM fact_order
            WHERE ABS(total - (subtotal + tax + shipping)) > 0.02
        """,
        "orphan_items": """
            SELECT COUNT(*) AS n
            FROM fact_order_item oi
            LEFT JOIN fact_order o ON o.order_id = oi.order_id
            WHERE o.order_id IS NULL
        """,
        "negative_money_rows": """
            SELECT COUNT(*) AS n
            FROM fact_order
            WHERE subtotal < 0 OR tax < 0 OR shipping < 0 OR total < 0
        """,
        "bad_line_total_rows": """
            SELECT COUNT(*) AS n
            FROM fact_order_item
            WHERE ABS(line_total - (qty * unit_price)) > 0.02
        """,
    }

    # Execute each check and store scalar results
    results: Dict[str, int] = {}
    with engine.connect() as conn:
        for name, q in checks.items():
            results[name] = int(conn.execute(text(q)).scalar() or 0)
    return results

# =============================================================================

def view_exists(engine: Engine, name: str) -> bool:
    """
    Check whether a database view exists in the current schema.

    Args:
        engine: SQLAlchemy engine
        name: View name

    Returns:
        True if view exists, False otherwise
    """

    sql = """
    SELECT COUNT(*)
    FROM information_schema.views
    WHERE table_schema = DATABASE() AND table_name = :name
    """
    with engine.connect() as conn:
        return int(conn.execute(text(sql), {"name": name}).scalar() or 0) > 0
    
# =============================================================================

def extract(engine: Engine) -> Dict[str, pd.DataFrame]:
    """
    Extract dimension, fact, and optional KPI views into DataFrames.

    Views are included only if they exist in the database.

    Args:
        engine: SQLAlchemy engine

    Returns:
        Dictionary mapping table or view name to pandas DataFrame
    """

    queries = {
        "dim_date": "SELECT * FROM dim_date",
        "dim_customer": "SELECT * FROM dim_customer",
        "dim_product": "SELECT * FROM dim_product",
        "fact_order": "SELECT * FROM fact_order",
        "fact_order_item": "SELECT * FROM fact_order_item",
        "fact_login": "SELECT * FROM fact_login",
    }

    # Optional analytics views
    if view_exists(engine, "vw_monthly_kpis"):
        queries["vw_monthly_kpis"] = """
            SELECT *
            FROM vw_monthly_kpis
            ORDER BY order_year, order_month
        """

    if view_exists(engine, "vw_monthly_mau"):
        queries["vw_monthly_mau"] = """
            SELECT *
            FROM vw_monthly_mau
            ORDER BY login_year, login_month
        """

    if view_exists(engine, "vw_customer_metrics"):
        queries["vw_customer_metrics"] = """
            SELECT *
            FROM vw_customer_metrics
            ORDER BY lifetime_value_completed DESC
        """

    # Execute queries and load into DataFrames
    dfs: Dict[str, pd.DataFrame] = {}
    for name, q in queries.items():
        dfs[name] = pd.read_sql(q, engine)
    return dfs

# =============================================================================

def write_outputs(dfs: Dict[str, pd.DataFrame], out_dir: str) -> None:
    """
    Write extracted DataFrames to CSV and Parquet files.

    Args:
        dfs: Dictionary of DataFrames
        out_dir: Output directory path
    """
    os.makedirs(out_dir, exist_ok=True)

    for name, df in dfs.items():
        df.to_csv(os.path.join(out_dir, f"{name}.csv"), index=False)

    parquet_dir = os.path.join(out_dir, "parquet")
    os.makedirs(parquet_dir, exist_ok=True)
    for name, df in dfs.items():
        df.to_parquet(os.path.join(parquet_dir, f"{name}.parquet"), index=False)

# =============================================================================

def main() -> None:
    """
    Main execution workflow.
    - Loads configuration from environment variables
    - Validates output directory access
    - Creates database engine
    - Runs data quality checks
    - Extracts data
    - Writes outputs to disk
    """

    load_dotenv() # Load .env from the current working directory (project root)

    # Load database configuration from environment variables
    cfg = DBConfig(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        db=os.getenv("DB_NAME", "ops_portfolio"),
    )

    print(">>> CONFIG <<<")
    print("Host: ", cfg.host)
    print("Port: ", cfg.port)
    print("User: ",cfg.user)
    print("DB: ",cfg.db)

    # Fail fast if password is missing
    if not cfg.password:
        raise ValueError("DB_PASSWORD is empty. Check your .env file.")

    # Resolve output directory
    output_dir = os.getenv("OUTPUT_DIR", "./output")

    print("CWD:", os.getcwd())
    print("OUTPUT_DIR:", output_dir)

    abs_out = os.path.abspath(output_dir)
    pathlib.Path(abs_out).mkdir(parents=True, exist_ok=True)

    # Test write permissions
    test_path = os.path.join(abs_out, "_write_test.txt")
    with open(test_path, "w", encoding="utf-8") as f:
        f.write("ETL write test OK\n")

    print(">>> WRITE TEST OK <<<", test_path)

    # Create DB engine
    engine = get_engine(cfg)
    print(">>> ENGINE CREATED <<<")

    # Run data quality checks
    check_results = run_checks(engine)
    print(">>> DATA QUALITY CHECKS <<<", check_results)

    # Extract tables and views
    dfs = extract(engine)
    print(">>> EXTRACTED TABLES <<<", list(dfs.keys()))

    # Write output files
    write_outputs(dfs, out_dir=abs_out)
    print(">>> EXPORT COMPLETE <<<")

    print("Files written to:", abs_out)
    for k in sorted(dfs.keys()):
        print(f"  - {k}.csv")

# =============================================================================

# Entry point guard
if __name__ == "__main__":
    main()

# =============================================================================
