import os, sys, time, logging
import psycopg2
import pandas as pd
import hashlib
import random
import io

# --------------------------------------------------
# Logging
# --------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger(__name__)

# --------------------------------------------------
# Environment validation (FAIL FAST)
# --------------------------------------------------
REQUIRED_ENV = [
    "POSTGRES_HOST",
    "STOCK_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_PORT",
    "CSV_PATH",
]

missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
if missing:
    logger.error(f"Missing required environment variables: {missing}")
    sys.exit(1)

DB = {
    "host": os.getenv("POSTGRES_HOST"),
    "dbname": os.getenv("STOCK_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "port": int(os.getenv("POSTGRES_PORT")),
    "connect_timeout": 10,
}

CSV_PATH = os.getenv("CSV_PATH")

MAX_RETRIES = 5
BASE_DELAY = 2

# --------------------------------------------------
# Helper functions
# --------------------------------------------------
def compute_file_hash(path: str) -> str:
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha.update(chunk)
    return sha.hexdigest()


def get_input_row_count(path: str) -> int:
    with open(path, newline="") as f:
        return sum(1 for _ in f) - 1  # exclude header

def retry_operation(operation, max_retries=5, base_delay=2):
    """
    Retry a function with exponential backoff + jitter.
    """
    for attempt in range(1, max_retries + 1):
        try:
            return operation()
        except Exception as e:
            if attempt == max_retries:
                logger.error(f"Max retries reached ({max_retries}). Aborting.")
                raise
            wait_time = base_delay * (2 ** (attempt - 1))
            wait_time += random.uniform(0, 1)
            logger.warning(
                f"Attempt {attempt} failed: {e}. "
                f"Retrying in {round(wait_time,2)} seconds..."
            )
            time.sleep(wait_time)

def connect_db():
    conn = psycopg2.connect(**DB)
    conn.autocommit = False
    return conn

def copy_chunk(cur, conn, buffer):
    """
    Perform COPY operation safely with rollback protection.
    """
    try:
        cur.copy_expert(
            """
            COPY raw_stock_prices
            (
                trade_timestamp,
                open_price,
                high_price,
                low_price,
                close_price,
                volume,
                bar_count,
                average_price
            )
            FROM STDIN WITH (FORMAT CSV)
            """,
            buffer
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

# --------------------------------------------------
# Main ingestion logic
# --------------------------------------------------
def main():
    logger.info("===== INGESTION STARTED =====")
    start_time = time.time()
    
    conn = None
    cur = None
    
    try:
        conn = retry_operation(connect_db, MAX_RETRIES, BASE_DELAY)
        cur = conn.cursor()
        

        # ---------- Metadata table ----------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS ingestion_metadata (
            dataset_hash TEXT PRIMARY KEY,
            row_count BIGINT,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        conn.commit()
        
        # --------------------------------------------------
        # Ensure feature table UNIQUE constraint exists
        # --------------------------------------------------
        logger.info("Ensuring UNIQUE index on stock_intraday_features")

        cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_spy_unique
        ON stock_intraday_features(trade_timestamp);
        """)
        conn.commit()

        # --------------------------------------------------
        # Guardrails
        # --------------------------------------------------
        logger.info("Running ingestion guardrails")

        # --- compute input dataset properties ---
        input_hash = compute_file_hash(CSV_PATH)
        input_row_count = get_input_row_count(CSV_PATH)

        logger.info(
            f"Input dataset fingerprint | hash={input_hash} rows={input_row_count}"
        )

        # Guardrail 1: same dataset already ingested
        cur.execute(
            "SELECT row_count FROM ingestion_metadata WHERE dataset_hash = %s;",
            (input_hash,)
        )
        meta = cur.fetchone()

        if meta:
            logger.info(
                f"Guardrail triggered: same input dataset already ingested "
                f"(hash={input_hash}, rows={meta[0]}). Skipping ingestion."
            )
            return 0

        # Guardrail 2: DB already fully covers input (safety fallback)
        cur.execute("SELECT COUNT(*) FROM raw_stock_prices;")
        existing_rows = cur.fetchone()[0]

        if existing_rows >= input_row_count:
            logger.info(
                f"Guardrail triggered: database already contains "
                f"{existing_rows} rows, input has {input_row_count}. "
                "Skipping ingestion."
            )
            return 0

        logger.info("Guardrails passed. Proceeding with ingestion.")

        # --------------------------------------------------
        # Truncate after guardrails passed
        # --------------------------------------------------
        logger.info("Truncating raw_stock_prices table")
        cur.execute("TRUNCATE TABLE raw_stock_prices;")
        conn.commit()

        # --------------------------------------------------
        # Ingestion
        # --------------------------------------------------
        csv_rows = 0
        chunk_count = 0

        for chunk in pd.read_csv(CSV_PATH, chunksize=200_000):
            chunk_count += 1
            
            # Normalize column names
            chunk.columns = chunk.columns.str.strip().str.lower()
            
            # Validate expected schema
            expected_columns = [
                "date", "open", "high", "low",
                "close", "volume", "barcount", "average"
            ]
            
            if not all(col in chunk.columns for col in expected_columns):
                raise ValueError("CSV schema does not match expected structure.")

            # Parse timestamp
            chunk["trade_timestamp"] = pd.to_datetime(
                chunk["date"],
                format="%Y%m%d %H:%M:%S"
            )
            
            chunk = chunk.rename(columns={
                "open":"open_price",
                "high":"high_price",
                "low":"low_price",
                "close":"close_price",
                "barcount": "bar_count",
                "average": "average_price"
            })
            
            # Drop original date column
            chunk = chunk.drop(columns=["date"])
            
            # Drop NA rows
            before = len(chunk)
            chunk = chunk.dropna()
            after = len(chunk)

            if before != after:
                logger.info(f"Dropped {before - after} rows due to NA values.")

            chunk = chunk[
                [
                    "trade_timestamp",
                    "open_price",
                    "high_price",
                    "low_price",
                    "close_price",
                    "volume",
                    "bar_count",
                    "average_price"
                ]
            ]
            
            # Convert DataFrame → in-memory CSV buffer
            buffer = io.StringIO()
            chunk.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            retry_operation(lambda: copy_chunk(cur, conn, buffer), MAX_RETRIES, BASE_DELAY)
            
            csv_rows += len(chunk)
            logger.info(
                f"Chunk {chunk_count} | rows_ingested={len(chunk)} | total_rows={csv_rows}")

        # --------------------------------------------------
        # DB Verification
        # --------------------------------------------------
        verification_status = "FAILED" # default assumption
        cur.execute("SELECT COUNT(*) FROM raw_stock_prices;")
        db_rows_readback = cur.fetchone()[0]

        if db_rows_readback != csv_rows:
            logger.error(
                f"Row-count mismatch: written={csv_rows}, "
                f"readback={db_rows_readback}"
            )
            return 1
        verification_status = "SUCCESS"
        
        # Analyze for query planner
        cur.execute("ANALYZE raw_stock_prices;")
        conn.commit()

        # ---------- Timing ----------
        end_time = time.time()
        elapsed_seconds = round(end_time - start_time, 2)

        # ---------- Summary ----------
        logger.info("===== INGESTION SUMMARY =====")
        logger.info(f"DB rows written         : {csv_rows}")
        logger.info(f"DB rows read-back       : {db_rows_readback}")
        logger.info(f"Verification status     : {verification_status}")
        logger.info(f"Total ingestion time(s) : {elapsed_seconds}")

        # --------------------------------------------------
        # Persist metadata (only after success)
        # --------------------------------------------------
        cur.execute(
            """
            INSERT INTO ingestion_metadata (dataset_hash, row_count)
            VALUES (%s, %s);
            """,
            (input_hash, input_row_count)
        )
        conn.commit()

        logger.info("===== INGESTION COMPLETED SUCCESSFULLY =====")

        return 0

    except Exception as e:
        # fail container so Airflow marks task FAILED
        logger.exception("Fatal ingestion failure – aborting batch run")
        return 1

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# --------------------------------------------------
# Entry point (EXIT CODE MATTERS)
# --------------------------------------------------
if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)