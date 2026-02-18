import os
import logging
import statistics
import json
from datetime import timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
from airflow.utils.state import State
from airflow.utils.timezone import utcnow
from docker.types import Mount

# ======================================================
# DEFAULT ARGS
# ======================================================
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# --------------------------------------------------
# Helper functions
# --------------------------------------------------

# ======================================================
# SUCCESS / FAILURE CALLBACKS
# ======================================================
def pipeline_success_callback(context):
    dag_run = context["dag_run"]
    start = dag_run.start_date
    end = dag_run.end_date or utcnow()
    duration = (end - start).total_seconds()

    logging.info("==============================================")
    logging.info("===== PIPELINE COMPLETED SUCCESSFULLY =====")
    logging.info("DAG ID       : %s", dag_run.dag_id)
    logging.info("Run ID       : %s", dag_run.run_id)
    logging.info("Execution TS : %s", dag_run.execution_date)
    logging.info("Duration     : %s seconds", duration)
    logging.info("Stages       : ingestion → analytics → verification (ALL PASSED)")
    logging.info("==============================================")

  
def pipeline_failure_callback(context):
    dag_run = context["dag_run"]

    failed_tasks = [
        ti.task_id
        for ti in dag_run.get_task_instances()
        if ti.state == State.FAILED
    ]

    logging.error("==============================================")
    logging.error("===== PIPELINE FAILED =====")
    logging.error("DAG ID       : %s", dag_run.dag_id)
    logging.error("Run ID       : %s", dag_run.run_id)
    logging.error("Execution TS : %s", dag_run.execution_date)

    if failed_tasks:
        logging.error("Failed stages:")
        for task in failed_tasks:
            logging.error(" - %s", task)
    else:
        logging.error(
            "Failure detected, but no failed task instances were found "
            "(possible upstream termination or manual stop)."
        )

    logging.error("Next steps:")
    logging.error(" - Open the failed task(s) in Airflow UI")
    logging.error(" - Review container / DB logs for root cause")
    logging.error("==============================================")

# ======================================================
# FINALIZER
# ======================================================
def pipeline_finalizer(**context):
    ti = context["ti"]
    verdict = ti.xcom_pull(
        task_ids="verify_analytics",
        key="verification_verdict"
    )

    if not verdict:
        logging.error("PIPELINE FAILED: no verification verdict found")
        raise ValueError("Pipeline failed: missing verification verdict")

    logging.info("===== PIPELINE VERIFICATION SUMMARY =====")
    logging.info("Distinct Raw rows: %s", verdict["raw_rows_distinct"])
    logging.info("Analytics rows: %s", verdict["analytics_rows"])
    logging.info("Row coverage ratio over 95 percent: %s", verdict["row_coverage_ratio"])
    logging.info("Date range OK: %s", verdict["date_range_ok"])
    logging.info("Crash index OK: %s", verdict["crash_index_ok"])
    logging.info("Sample read OK: %s", verdict["sample_read_ok"])

    if verdict["error"]:
        logging.error("PIPELINE FAILED: %s", verdict["error"])
        raise ValueError("Pipeline failed verification")

    logging.info("===== PIPELINE COMPLETED SUCCESSFULLY =====")
    
# --------------------------------------------------
# Drift Detection Utility
# --------------------------------------------------
def detect_drift(current, historical, label, threshold=3):
    """
    Detect statistical drift using Z-score.
    Returns True if drift detected, else False.
    """
    if current is None or len(historical) < 2:
        return False

    mean_h = statistics.mean(historical)
    std_h = statistics.pstdev(historical)

    if std_h == 0:
        return False

    z = abs((current - mean_h) / std_h)

    if z > threshold:
        logging.warning("%s drift detected (z=%.2f)", label, z)
        return True

    return False

# ======================================================
# VERIFICATION TASK
# =====================================================
def verify_analytics_task(**context):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    ti = context["ti"]
    dag_run = context["dag_run"]
    run_id = dag_run.run_id
    
    verdict = {
        "raw_rows_distinct": None,
        "analytics_rows": None,
        "row_coverage_ratio": False,
        "date_range_ok": False,
        "crash_index_ok": False,
        "sample_read_ok": False,
        "checksum": None,
        "error": None,
    }
    try:
        # --------------------------------------------------
        # 1.Row Counts Check
        # --------------------------------------------------
        raw_rows_distinct = hook.get_first("SELECT COUNT(DISTINCT trade_timestamp) FROM raw_stock_prices")[0]
        analytics_rows = hook.get_first("SELECT COUNT(*) FROM stock_intraday_features")[0]

        verdict["raw_rows_distinct"] = raw_rows_distinct
        verdict["analytics_rows"] = analytics_rows

        # --------------------------------------------------
        # 2.Hard checks
        # --------------------------------------------------
        if analytics_rows <= 0:
            raise ValueError("Verification failed: analytics table empty")
        if analytics_rows > raw_rows_distinct:
            raise ValueError(
                "Row overflow detected : analytics rows exceed raw rows"
                f"(raw={raw_rows_distinct}, analytics={analytics_rows})"
            )

        # Allow rolling-window removal tolerance (~5%)
        coverage_ratio = analytics_rows / raw_rows_distinct if raw_rows_distinct else 0

        if coverage_ratio < 0.95:
            raise ValueError (
                f"Incomplete analytics table : partial write detected "
                f"(coverage={coverage_ratio:.2%},raw={raw_rows_distinct}, analytics={analytics_rows})"
            )
        
        verdict["row_coverage_ratio"] = True
        
        # --------------------------------------------------
        # 3.Batched Integrity Checks (single DB round-trip)
        # --------------------------------------------------
        metrics = hook.get_first("""
            WITH base AS (
                SELECT
                    trade_timestamp,
                    trade_date,
                    crash_index,
                    crash_alert_flag,
                    crash_severity,
                    drawdown,
                    volatility_30min,
                    volume_ratio,
                    volatility_zscore
                FROM stock_intraday_features
            ),
            duplicates AS (
                SELECT COUNT(*) AS duplicate_count
                FROM (
                    SELECT trade_timestamp
                    FROM base
                    GROUP BY trade_timestamp
                    HAVING COUNT(*) > 1
                ) t
            ),
            gaps AS (
                SELECT COUNT(*) AS gap_count
                FROM (
                    SELECT
                        trade_timestamp,
                        trade_date,
                        LAG(trade_timestamp)
                            OVER (PARTITION BY trade_date ORDER BY trade_timestamp) AS prev_ts
                    FROM base
                ) t
                WHERE prev_ts IS NOT NULL
                  AND trade_timestamp - prev_ts > INTERVAL '1 minute'
            )
            
            SELECT
                MIN(trade_date),
                MAX(trade_date),
                COALESCE(SUM(CASE WHEN crash_index IS NULL THEN 1 ELSE 0 END),0),
                COALESCE(SUM(CASE WHEN crash_alert_flag = 1 AND crash_severity = 'LOW' THEN 1 ELSE 0 END),0),
                COALESCE(SUM(CASE WHEN drawdown > 0 THEN 1 ELSE 0 END),0),
                COALESCE(SUM(CASE WHEN volatility_30min < 0 THEN 1 ELSE 0 END),0),
                COALESCE(SUM(CASE WHEN volume_ratio < 0 THEN 1 ELSE 0 END),0),
                COALESCE(SUM(CASE WHEN ABS(volatility_zscore) > 10 THEN 1 ELSE 0 END),0),
                COALESCE(SUM(CASE WHEN ABS(crash_index) > 50 THEN 1 ELSE 0 END),0),
                (SELECT duplicate_count FROM duplicates),
                (SELECT gap_count FROM gaps)

            FROM base;
        """)

        (
            min_analytics,
            max_analytics,
            null_crash,
            alert_mismatch,
            positive_drawdown,
            negative_vol,
            negative_volume_ratio,
            extreme_zscore,
            extreme_crash,
            duplicate_count,
            gap_count
        ) = metrics
        
        # --------------------------------------------------
        # 4.Date range
        # --------------------------------------------------
        min_raw, max_raw = hook.get_first(
            "SELECT MIN(DATE(trade_timestamp)), MAX(DATE(trade_timestamp)) FROM raw_stock_prices"
        )
        
        if min_raw is None or max_raw is None:
            raise ValueError("Verification failed: Invalid raw date range")
        
        if min_analytics is None or max_analytics is None:
            raise ValueError("Verification failed: Invalid analytics date range")
        
        if min_raw != min_analytics or max_raw != max_analytics:
            raise ValueError("Verification failed: Date range coverage mismatch")

        verdict["date_range_ok"] = True
        
        # ==================================================
        # 5.LOGICAL AND STRUCTURAL INTEGRITY VALIDATION
        # ==================================================
        if any([
            null_crash,
            alert_mismatch,
            positive_drawdown,
            negative_vol,
            negative_volume_ratio,
            duplicate_count,
            gap_count
        ]):
            raise ValueError("Logical integrity validation failed")

        verdict["crash_index_ok"] = True

        # ==================================================
        # 6.STATISTICAL DRIFT WARNING
        # ==================================================
        if extreme_zscore > 0:
            logging.warning("Extreme volatility_zscore values detected")

        if extreme_crash > 0:
            logging.warning("Extreme crash_index values detected")
        
        # ==================================================
        # 7.CHECKSUM FINGERPRINT (Deterministic)
        # ==================================================
        checksum = hook.get_first("""
            SELECT MD5(
                STRING_AGG(
                    trade_timestamp::text || '|' ||
                    close_price::text || '|' ||
                    crash_index::text || '|' ||
                    crash_alert_flag::text,
                    '||'
                    ORDER BY trade_timestamp
                )
            )
            FROM stock_intraday_features
        """)[0]

        verdict["checksum"] = checksum

        # ==================================================
        # 8.DRIFT DETECTION (Threshold + Alert Rate)
        # ==================================================

        optimal_threshold = None
        alert_rate = None
        crash_index_mean = None
        crash_index_std = None

        drift_flag = False
        
        metrics_path = "/opt/spark/output/metrics.json"
        try:
            if not os.path.exists(metrics_path):
                raise FileNotFoundError("metrics.json not found")
            
            with open(metrics_path, "r") as f:
                metrics = json.load(f)

            optimal_threshold = metrics.get("optimal_threshold")
            alert_rate = metrics.get("alert_rate")
            crash_index_mean = metrics.get("crash_index_mean")
            crash_index_std = metrics.get("crash_index_std")

            logging.info(
                "Spark metrics extracted: threshold=%s alert_rate=%s mean=%s std=%s",
                optimal_threshold,
                alert_rate,
                crash_index_mean,
                crash_index_std
            )
        
        except Exception as e:
            logging.warning("Drift detection skipped — failed to load metrics file: %s", str(e))
        
        # Proceed only if metrics successfully parsed
        if None not in (optimal_threshold, alert_rate, crash_index_mean, crash_index_std):
            
            previous_runs = hook.get_records("""
                SELECT optimal_threshold, alert_rate,
                       crash_index_mean, crash_index_std
                FROM pipeline_run_metadata
                ORDER BY execution_ts DESC
                LIMIT 5
            """)

            if previous_runs:

                prev_thresholds = [r[0] for r in previous_runs if r[0] is not None]
                prev_alert_rates = [r[1] for r in previous_runs if r[1] is not None]
                prev_means = [r[2] for r in previous_runs if r[2] is not None]
                prev_stds = [r[3] for r in previous_runs if r[3] is not None]

                drift_flag |= detect_drift(optimal_threshold, prev_thresholds, "Threshold")
                drift_flag |= detect_drift(alert_rate, prev_alert_rates, "Alert rate")
                drift_flag |= detect_drift(crash_index_mean, prev_means, "Crash index mean")
                drift_flag |= detect_drift(crash_index_std, prev_stds, "Crash index std")
                
                # ===============================
                # Drift Analysis Summary Logging
                # ===============================
                logging.info("=== DRIFT ANALYSIS SUMMARY ===")
                logging.info("Current threshold: %s", optimal_threshold)

                if prev_thresholds:
                    logging.info("Historical threshold mean: %s", statistics.mean(prev_thresholds))
                else:
                    logging.info("Historical threshold mean: N/A (insufficient history)")

                logging.info("Drift detected: %s", drift_flag)

            if drift_flag:
                logging.warning("Behavioral drift detected — review recommended.")
        else:
            logging.warning("Drift detection skipped — Spark metadata missing")
               
        # ==================================================
        # 9. METADATA PERSISTENCE
        # ==================================================
        hook.run("""
            INSERT INTO pipeline_run_metadata
            (run_id, raw_rows_distinct, analytics_rows, data_checksum,
             optimal_threshold, alert_rate,
             crash_index_mean, crash_index_std,drift_detected)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (run_id) DO UPDATE
            SET raw_rows_distinct = EXCLUDED.raw_rows_distinct,
                analytics_rows = EXCLUDED.analytics_rows,
                data_checksum = EXCLUDED.data_checksum,
                optimal_threshold = EXCLUDED.optimal_threshold,
                alert_rate = EXCLUDED.alert_rate,
                crash_index_mean = EXCLUDED.crash_index_mean,
                crash_index_std = EXCLUDED.crash_index_std,
                execution_ts = CURRENT_TIMESTAMP,
                drift_detected = EXCLUDED.drift_detected
        """, parameters=(
            run_id,
            raw_rows_distinct,
            analytics_rows,
            checksum,
            optimal_threshold,
            alert_rate,
            crash_index_mean,
            crash_index_std,
            drift_flag
        ))

        # --------------------------------------------------
        # 10.Sample read check
        # --------------------------------------------------
        try:
            samples = hook.get_records("""
                SELECT trade_timestamp, close_price, crash_index, crash_alert_flag, crash_severity
                FROM stock_intraday_features
                ORDER BY trade_timestamp DESC
                LIMIT 10
            """)

        except Exception as e:
            raise ValueError(
                "Verification failed: unable to read analytics sample "
                f"(trade_timestamp, crash_index) | DB error: {str(e)}"
            ) from e
        
        verdict["sample_read_ok"] = True
        logging.info("Sample verification passed. Rows fetched: %s", len(samples))

        # ---- LOG OUTPUT (observable, unchanged intent) ----
        logging.info("===== DB VERIFICATION COMPLETED SUCCESSFULLY =====")
        logging.info(
            "Verification raw_rows_distinct=%s analytics_rows=%s row_coverage_ratio_satisfied=%s",
            raw_rows_distinct, analytics_rows, verdict["row_coverage_ratio"]
        )
        logging.info(
            "Analytics date range=%s → %s",
            min_analytics, max_analytics
        )

        logging.info("Sample analytics rows:")
        for r in samples:
            logging.info("TS=%s | Close=%s | CI=%s | Alert=%s | Severity=%s",
            r[0], r[1], r[2], r[3], r[4]
            )

        ti.xcom_push(key="verification_verdict", value=verdict)

    except Exception as e:
        verdict["error"] = str(e)
        ti.xcom_push(key="verification_verdict", value=verdict)
        raise

# ======================================================
# DAG DEFINITION
# ======================================================
with DAG(
    dag_id="stock_batch_pipeline",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,  # sequential execution
    dagrun_timeout=timedelta(minutes=20),
    tags=["batch", "spark", "postgres"],
    on_success_callback=pipeline_success_callback,
    on_failure_callback=pipeline_failure_callback,
) as dag:

    pg_conn = BaseHook.get_connection("postgres_default")
    
    # --------------------------------------------------
    # 1.Wait for Postgres
    # --------------------------------------------------
    wait_for_postgres = SqlSensor(
        task_id="wait_for_postgres",
        conn_id="postgres_default",
        sql="SELECT 1;",
        timeout=120,
        poke_interval=5,
        mode="reschedule",
    )

    # --------------------------------------------------
    # 2.Ingestion
    # --------------------------------------------------
    run_ingestion = DockerOperator(
        task_id="run_ingestion",
        image="batch-stock-market-pipeline-ingestion",
        api_version="auto",
        command=["python", "ingest.py"],
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="stock_data_pipeline",
        force_pull=False,
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source="stock_data_airflow_data",
                target="/data",
                type="volume",
                read_only=True,
                )
        ],
        environment={
        "POSTGRES_HOST": pg_conn.host.strip(),
        "STOCK_DB": pg_conn.schema.strip(),
        "POSTGRES_USER": pg_conn.login.strip(),
        "POSTGRES_PASSWORD": pg_conn.password.strip(),
        "POSTGRES_PORT": str(pg_conn.port),
        "CSV_PATH": os.environ.get("CSV_PATH"),
        },
        # Retry policy
        retries=2,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=6),
        execution_timeout=timedelta(minutes=30),
        # Resource controls
        mem_limit="2g",
    )

    # --------------------------------------------------
    # 3.Spark Batch Analytics
    # --------------------------------------------------
    run_spark = DockerOperator(
        task_id="run_spark",
        image="batch-stock-market-pipeline-spark",
        api_version="auto",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="stock_data_pipeline",
        mount_tmp_dir=False,
        environment={
        "POSTGRES_HOST": pg_conn.host.strip(),
        "STOCK_DB": pg_conn.schema.strip(),
        "POSTGRES_USER": pg_conn.login.strip(),
        "POSTGRES_PASSWORD": pg_conn.password.strip(),
        "POSTGRES_PORT": str(pg_conn.port),
        },
        mounts=[
            Mount(
                source="spark_output",
                target="/opt/spark/output",
                type="volume",
                )
        ],
        # Retry policy
        retries=3,
        retry_delay=timedelta(minutes=3),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=10),
        execution_timeout=timedelta(minutes=20),
        do_xcom_push=True,
        # SLA monitoring
        sla=timedelta(minutes=25),
        # Resource controls
        mem_limit="4g",
    )

    # --------------------------------------------------
    # 4.Database Verification
    # --------------------------------------------------
    verify_analytics = PythonOperator(
        task_id="verify_analytics",
        python_callable=verify_analytics_task,
        provide_context=True,
        retries=0,
    )
    
    # --------------------------------------------------
    # 5.Pipeline Finalization
    # --------------------------------------------------
    finalize_pipeline = PythonOperator(
        task_id="finalize_pipeline",
        python_callable=pipeline_finalizer,
    )

    wait_for_postgres >> run_ingestion >> run_spark >> verify_analytics >> finalize_pipeline
