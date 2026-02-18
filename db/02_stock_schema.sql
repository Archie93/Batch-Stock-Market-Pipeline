-- =====================================================
-- Database Initialization Script
-- Project: SPY Intraday Crash Detection Pipeline
-- =====================================================

-- Switch to stockdb before creating tables
\connect stockdb;
-- -----------------------------------------------------
-- 1. RAW STOCK PRICES TABLE
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_stock_prices (
    trade_timestamp TIMESTAMP NOT NULL,
    open_price DOUBLE PRECISION,
    high_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    close_price DOUBLE PRECISION,
    volume BIGINT,
	bar_count INTEGER,
    average_price DOUBLE PRECISION
);

-- Index for faster time-based queries
CREATE INDEX IF NOT EXISTS idx_raw_timestamp
ON raw_stock_prices(trade_timestamp);

-- -----------------------------------------------------
-- 2. FEATURE TABLE (INTRADAY ANALYTICS)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS stock_intraday_features (
    trade_timestamp TIMESTAMP NOT NULL,
    trade_date DATE NOT NULL,
    open_price DOUBLE PRECISION,
    high_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    close_price DOUBLE PRECISION,
    volume BIGINT,
	
	-- Core Returns
    log_return DOUBLE PRECISION,
	
	-- Momentum
    momentum_5min DOUBLE PRECISION,
	momentum_15min DOUBLE PRECISION,
	
	-- Volatility
	volatility_15min DOUBLE PRECISION,
    volatility_30min DOUBLE PRECISION,
    volatility_60min DOUBLE PRECISION,
	
	-- Volume Metrics
    avg_volume_30min DOUBLE PRECISION,
    volume_ratio DOUBLE PRECISION,
	
	-- VWAP (Volume Weighted Average Price)
	vwap_15min DOUBLE PRECISION,
    vwap_30min DOUBLE PRECISION,
	
	-- Price Range
    avg_range_15min DOUBLE PRECISION,
	
	-- Risk Metrics
    rolling_peak DOUBLE PRECISION,
    drawdown DOUBLE PRECISION,
    var_95_proxy DOUBLE PRECISION,
	
	-- Volatility Normalization
    volatility_mean_60 DOUBLE PRECISION,
    volatility_std_60 DOUBLE PRECISION,
    volatility_zscore DOUBLE PRECISION,
	
	-- Crash Detection
    crash_index DOUBLE PRECISION,
    true_crash INTEGER,
    crash_alert_flag INTEGER,
    crash_severity TEXT
);

-- -----------------------------------------------------
-- 3. UNIQUE CONSTRAINT (Prevents Duplicate Writes)
-- -----------------------------------------------------
CREATE UNIQUE INDEX IF NOT EXISTS idx_spy_unique
ON stock_intraday_features(trade_timestamp);

-- -----------------------------------------------------
-- 4. PERFORMANCE INDEXES
-- -----------------------------------------------------

-- Fast date filtering
CREATE INDEX IF NOT EXISTS idx_feature_trade_date
ON stock_intraday_features(trade_date);

-- Fast severity filtering
CREATE INDEX IF NOT EXISTS idx_feature_severity
ON stock_intraday_features(crash_severity);

-- -----------------------------------------------------
-- 5. INGESTION METADATA TABLE
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS ingestion_metadata (
    dataset_hash TEXT PRIMARY KEY,
    row_count BIGINT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------
-- 6. SPARK METADATA TABLE
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_run_metadata (
    run_id TEXT PRIMARY KEY,
    raw_rows_distinct BIGINT,
    analytics_rows BIGINT,
    data_checksum TEXT,
    optimal_threshold DOUBLE PRECISION,
    alert_rate DOUBLE PRECISION,
    crash_index_mean DOUBLE PRECISION,
    crash_index_std DOUBLE PRECISION,
    execution_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	drift_detected BOOLEAN DEFAULT FALSE
);

-- -----------------------------------------------------
-- Initialization Complete
-- -----------------------------------------------------