import logging, os, time, json
import builtins
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, to_date, avg, lag, sum, stddev, abs, log, max, when, count)
from pyspark.sql.window import Window

# --------------------------------------------------
# Logging
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)
# --------------------------------------------------
# Environment variables
# --------------------------------------------------
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB   = os.getenv("STOCK_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD")

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

JDBC_PROPS = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASS,
    "driver": "org.postgresql.Driver"
}

SOURCE_TABLE = "raw_stock_prices"
TARGET_TABLE = "stock_intraday_features"

JDBC_PARTITIONS = 6
JDBC_BATCH_SIZE = 15000

def main():
    spark = None
    try:
        job_start = time.time()

        # --------------------------------------------------
        # 1. Spark session
        # --------------------------------------------------
        # 

        spark = (
            SparkSession.builder
            .appName("StockIntradayAnalytics")
            .getOrCreate()
        )

        logger.info("===== SPARK Session STARTED =====")

        # --------------------------------------------------
        # 2. Read from Postgres (Partitioned)
        # --------------------------------------------------
        df = (
            spark.read
            .format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", SOURCE_TABLE)
            .option("user", JDBC_PROPS["user"])
            .option("password", JDBC_PROPS["password"])
            .option("driver", JDBC_PROPS["driver"])
            .load()
        )
        
        # --------------------------------------------------
        # 3. Read daily stock data
        # --------------------------------------------------
        
        logger.info("===== SPARK Source table Read COMPLETED =====")
        
        raw_dup = (
            df
            .groupBy("trade_timestamp")
            .count()
            .filter("count > 1")
        )
        raw_dup_count = raw_dup.count()
        print("Raw duplicates:", raw_dup.count())

        if raw_dup_count > 0:
            logger.warning(f"Dropping {raw_dup_count} duplicate raw timestamps")
            df = df.dropDuplicates(["trade_timestamp"])
        
        df = df.withColumn("trade_date", to_date(col("trade_timestamp")))

        input_rows = df.count()
        if input_rows == 0:
            raise RuntimeError("No data available in raw table.")
        
        date_range = df.selectExpr(
            "min(trade_date) as min_date",
            "max(trade_date) as max_date"
        ).collect()[0]

        logger.info(
            f"Spark input verification | "
            f"rows={input_rows} | "
            f"date_range={date_range.min_date} → {date_range.max_date}"
        )

        logger.info(f"Input rows: {input_rows}")

        # --------------------------------------------------
        # 4. Base validation (daily data)
        # --------------------------------------------------
        logger.info("===== SPARK Read Validation STARTED =====")
        df = (
            df
            .filter(col("trade_date").isNotNull())
            .filter(col("open_price") >= 0)
            .filter(col("high_price") >= 0)
            .filter(col("low_price") >= 0)
            .filter(col("close_price") >= 0)
        )
        logger.info("===== SPARK Read Validation COMPLETED =====")

        # --------------------------------------------------
        # 5. Window definitions
        # --------------------------------------------------
        # Intraday partitioning
        w_order = Window.partitionBy("trade_date").orderBy("trade_timestamp")

        # Minute-based windows
        w_5min  = w_order.rowsBetween(-4, 0)
        w_15min = w_order.rowsBetween(-14, 0)
        w_30min = w_order.rowsBetween(-29, 0)
        w_60min = w_order.rowsBetween(-59, 0)

        # --------------------------------------------------
        # 6. Rolling analytics
        # --------------------------------------------------
        logger.info("===== SPARK Rolling Analytics STARTED =====")

        # Daily return
        df = df.withColumn(
            "prev_close",
            lag("close_price", 1).over(w_order)
        )
        df = df.withColumn(
            "log_return",when(col("prev_close") > 0,
            log(col("close_price")/col("prev_close")))
        )
        
        df = df.drop("prev_close")
        
        df = df.filter(col("log_return").isNotNull())

        # Moving averages and Rolling volatility (15,30 and 60 mins)
        df = (
            df
            .withColumn("momentum_5min", avg("log_return").over(w_5min))
            .withColumn("momentum_15min", avg("log_return").over(w_15min))
            .withColumn("volatility_15min", stddev("log_return").over(w_15min))
            .withColumn("volatility_30min", stddev("log_return").over(w_30min))
            .withColumn("volatility_60min", stddev("log_return").over(w_60min))
            .withColumn("avg_volume_30min", avg("volume").over(w_30min))
            .withColumn("volume_ratio",when(col("avg_volume_30min") > 0,col("volume") / col("avg_volume_30min")).otherwise(0))
        )
        
        # VWAP
        df = df.withColumn(
            "vwap_15min",
            sum(col("close_price") * col("volume")).over(w_15min) /
            sum("volume").over(w_15min)
        )

        df = df.withColumn(
            "vwap_30min",
            sum(col("close_price") * col("volume")).over(w_30min) /
            sum("volume").over(w_30min)
        )
        
        # Avg price range
        df = df.withColumn(
            "avg_range_15min",
            avg(col("high_price") - col("low_price")).over(w_15min)
        )
        
        # --------------------------------------------------
        # 7. CRASH INDEX COMPONENTS
        # 7.1 Volatility Z-score (normalize stress)
        # --------------------------------------------------
        w_60min = Window.partitionBy("trade_date") \
                        .orderBy("trade_timestamp") \
                        .rowsBetween(-59, 0)

        df = df.withColumn(
            "volatility_mean_60",
            avg("volatility_30min").over(w_60min)
        )

        df = df.withColumn(
            "volatility_std_60",
            stddev("volatility_30min").over(w_60min)
        )

        df = df.withColumn(
            "volatility_zscore",
            when(
                col("volatility_std_60") != 0,
                (col("volatility_30min") - col("volatility_mean_60")) /
                col("volatility_std_60")
            ).otherwise(0)
        )

        # --------------------------------------------------
        # 7.2 Drawdown (intraday peak drop)
        # --------------------------------------------------
        w_cummax = Window.partitionBy("trade_date") \
                         .orderBy("trade_timestamp") \
                         .rowsBetween(Window.unboundedPreceding, 0)

        df = df.withColumn(
            "rolling_peak",
            max("close_price").over(w_cummax)
        )

        df = df.withColumn(
            "drawdown",
            (col("close_price") - col("rolling_peak")) /
            col("rolling_peak")
        )
        
        df = df.withColumn(
            "var_95_proxy",
            col("volatility_30min") * 1.65
        )

        # --------------------------------------------------
        # 8. COMPOSITE CRASH INDEX
        # --------------------------------------------------
        df = df.withColumn(
            "crash_index",
            0.35 * col("volatility_zscore") +
            0.25 * col("volume_ratio") +
            0.25 * abs(col("drawdown")) +
            0.15 * abs(col("log_return"))
        )
        # Cache before repeated threshold computations
        df = df.cache()
        df.count()
        
        # --------------------------------------------------
        # 9. BACKTESTING ENGINE
        # --------------------------------------------------
        df = df.withColumn(
            "true_crash",
            when(col("drawdown") < -0.03, 1).otherwise(0)
        )

        # --------------------------------------------------
        # 10. AUTOMATIC THRESHOLD SELECTION (F1 Optimized)
        # --------------------------------------------------
        threshold_values = [x * 0.5 for x in range(1, 11)]  # 0.5 → 5.0
        results = []

        for T in threshold_values:
            temp_df = df.withColumn(
                "predicted_crash",
                when(col("crash_index") > T, 1).otherwise(0)
            )

            metrics = temp_df.selectExpr(
                "sum(case when predicted_crash=1 and true_crash=1 then 1 else 0 end) as TP",
                "sum(case when predicted_crash=1 and true_crash=0 then 1 else 0 end) as FP",
                "sum(case when predicted_crash=0 and true_crash=1 then 1 else 0 end) as FN"
            ).collect()[0]

            TP = metrics.TP or 0
            FP = metrics.FP or 0
            FN = metrics.FN or 0

            precision = TP / (TP + FP) if (TP + FP) > 0 else 0
            recall = TP / (TP + FN) if (TP + FN) > 0 else 0
            f1 = (2 * precision * recall) / (precision + recall) \
                if (precision + recall) > 0 else 0

            results.append((T, f1))

        OPTIMAL_THRESHOLD = builtins.max(results, key=lambda x: x[1])[0]

        logger.info(f"Optimal threshold selected: {OPTIMAL_THRESHOLD}")

        # --------------------------------------------------
        # 11. Crash Alert Flag
        # --------------------------------------------------
        df = df.withColumn(
            "crash_alert_flag",
            when(col("crash_index") > OPTIMAL_THRESHOLD, 1).otherwise(0)
        )

        # --------------------------------------------------
        # 12. SEVERITY LEVELS
        # --------------------------------------------------
        df = df.withColumn(
            "crash_severity",
            when(col("crash_index") <= OPTIMAL_THRESHOLD, "LOW")
            .when(col("crash_index") <= 1.5 * OPTIMAL_THRESHOLD, "MEDIUM")
            .when(col("crash_index") <= 2.0 * OPTIMAL_THRESHOLD, "HIGH")
            .otherwise("CRITICAL")
        )

        # --------------------------------------------------
        # 13. FEATURE STORE WRITE
        # --------------------------------------------------
        final_df = df.select(
            "trade_timestamp","trade_date",
            "open_price","high_price","low_price","close_price","volume",
            "log_return","momentum_5min","momentum_15min",
            "volatility_15min","volatility_30min","volatility_60min",
            "volume_ratio","vwap_15min","vwap_30min","avg_range_15min",
            "rolling_peak","drawdown","var_95_proxy",
            "volatility_mean_60","volatility_std_60","volatility_zscore",
            "crash_index","true_crash","crash_alert_flag","crash_severity"
        )

        # Cache before validation
        final_df = final_df.cache()

        final_rows = final_df.count()
        logger.info(f"Rows to write: {final_rows}")
        # --------------------------------------------------
        # 14. Data validation check for analytics table
        # --------------------------------------------------
        # Basic Null Sanity Checks
        null_checks = final_df.selectExpr(
            "sum(case when log_return is null then 1 else 0 end) as null_returns",
            "sum(case when momentum_5min is null then 1 else 0 end) as null_momentum_5min",
            "sum(case when momentum_15min is null then 1 else 0 end) as null_momentum_15min"
        ).collect()[0]

        logger.info(
            f"Analytics sanity check | "
            f"null_returns={null_checks.null_returns} | "
            f"null_momentum_5min={null_checks.null_momentum_5min} | "
            f"null_momentum_15min={null_checks.null_momentum_15min}"
        )
        
        # --------------------------------------------------
        # 15.Combined Validation (Single Pass)
        # --------------------------------------------------

        validation_metrics = final_df.selectExpr(

            # Structural checks
            "sum(case when trade_timestamp is null then 1 else 0 end) as null_timestamp",

            # Numeric sanity
            "sum(case when volatility_30min < 0 then 1 else 0 end) as negative_volatility",
            "sum(case when crash_index is null then 1 else 0 end) as null_crash_index",
            "sum(case when volume_ratio < 0 then 1 else 0 end) as negative_volume_ratio",

            # Statistical stability
            "sum(case when abs(volatility_zscore) > 10 then 1 else 0 end) as extreme_zscore",
            "sum(case when abs(crash_index) > 50 then 1 else 0 end) as extreme_crash_index",

            # Logical consistency
            "sum(case when crash_alert_flag = 1 and crash_severity = 'LOW' then 1 else 0 end) as severity_mismatch",
            "sum(case when drawdown > 0 then 1 else 0 end) as invalid_drawdown"

        ).collect()[0]

        logger.info(
            f"Validation Summary | "
            f"null_timestamp={validation_metrics.null_timestamp} | "
            f"negative_volatility={validation_metrics.negative_volatility} | "
            f"null_crash_index={validation_metrics.null_crash_index} | "
            f"negative_volume_ratio={validation_metrics.negative_volume_ratio} | "
            f"extreme_zscore={validation_metrics.extreme_zscore} | "
            f"extreme_crash_index={validation_metrics.extreme_crash_index} | "
            f"severity_mismatch={validation_metrics.severity_mismatch} | "
            f"invalid_drawdown={validation_metrics.invalid_drawdown}"
        )

        # --------------------------------------------------
        # Hard Fail Conditions
        # --------------------------------------------------

        if validation_metrics.null_timestamp > 0:
            raise RuntimeError("Validation failed: null trade_timestamp detected.")

        if validation_metrics.negative_volatility > 0:
            raise RuntimeError("Validation failed: negative volatility detected.")

        if validation_metrics.null_crash_index > 0:
            raise RuntimeError("Validation failed: null crash_index detected.")

        if validation_metrics.severity_mismatch > 0:
            raise RuntimeError("Validation failed: crash severity mismatch detected.")

        # --------------------------------------------------
        # Soft Warnings
        # --------------------------------------------------

        if validation_metrics.extreme_zscore > 0:
            logger.warning("Extreme volatility_zscore values detected.")

        if validation_metrics.extreme_crash_index > 0:
            logger.warning("Extreme crash_index values detected.")

        if validation_metrics.invalid_drawdown > 0:
            logger.warning("Positive drawdown detected (unexpected).")

        # Fail if unexpected corruption
        if null_checks.null_returns > 0:
            raise RuntimeError("Null log_return detected — aborting write.")
        
        total_rows = final_df.count()
        (
            final_df
            .groupBy("trade_timestamp")
            .count()
            .filter("count > 1")
            .orderBy("count", ascending=False)
            .show(20, truncate=False)
        )
        distinct_rows = final_df.dropDuplicates(["trade_timestamp"]).count()

        logger.info(f"Total rows: {total_rows}")
        logger.info(f"Distinct rows: {distinct_rows}")
        
        logger.info("===== SPARK Rolling Analytics COMPLETED =====")

        # --------------------------------------------------
        # 16. Write analytics table
        # --------------------------------------------------
        logger.info("===== SPARK Writing Analytics table =====")
        final_df = final_df.repartition(JDBC_PARTITIONS)

        (
            final_df
            .write
            .format("jdbc")
            .mode("overwrite")
            .option("url", JDBC_URL)
            .option("dbtable", TARGET_TABLE)
            .option("user", JDBC_PROPS["user"])
            .option("password", JDBC_PROPS["password"])
            .option("driver", JDBC_PROPS["driver"])
            #PERFORMANCE TUNING
            .option("batchsize", JDBC_BATCH_SIZE)
            .option("isolationLevel", "READ_COMMITTED")
            .option("truncate", "true")
            .save()
        )

        logger.info("===== SPARK Writing Analytics table completed=====")
        spark.catalog.clearCache()
        
        crash_stats = final_df.selectExpr(
            "avg(crash_index) as mean_ci",
            "stddev(crash_index) as std_ci",
            "avg(crash_alert_flag) as alert_rate"
        ).collect()[0]

        crash_index_mean = crash_stats.mean_ci
        crash_index_std = crash_stats.std_ci
        alert_rate = crash_stats.alert_rate

        duration = time.time() - job_start
        logger.info(f"Spark analytics job completed in {duration:.2f} seconds")
        logger.info(
            f"Spark job completed successfully | "
            f"rows_written={final_rows} | "
            f"duration={duration:.2f}s"
        )
        metrics = {
            "optimal_threshold": round(OPTIMAL_THRESHOLD, 4),
            "alert_rate": round(alert_rate, 4),
            "crash_index_mean": round(crash_index_mean, 4),
            "crash_index_std": round(crash_index_std, 4)
        }

        output_dir = "/opt/spark/output"

        # Ensure directory exists
        os.makedirs(output_dir, exist_ok=True)

        metrics_path = os.path.join(output_dir, "metrics.json")

        with open(metrics_path, "w") as f:
            json.dump(metrics, f)
        
        logger.info(f"Metrics written to {metrics_path}")
        
        print(json.dumps(metrics), flush=True)

    except Exception as e:
        logger.exception("Spark analytics job failed")
        raise

    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()