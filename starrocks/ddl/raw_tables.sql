CREATE DATABASE IF NOT EXISTS market_db;
USE market_db;

CREATE TABLE IF NOT EXISTS raw_daily_bar(
    symbol VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL, 
    open DECIMAL(18, 4),
    high DECIMAL(18, 4),
    low DECIMAL(18, 4),
    close DECIMAL(18, 4),
    volume BIGINT,
    updated_at DATETIME,
    op VARCHAR(10), 
    op_ts BIGINT, 
    _ingest_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
PRIMARY KEY (symbol, trade_date)
DISTRIBUTED BY HASH(symbol) BUCKETS 4 PROPERTIES("replication_num" = "1");

CREATE TABLE IF NOT EXISTS raw_instrument(
    symbol VARCHAR(20) NOT NULL, 
    name VARCHAR(255),
    exchange VARCHAR(20),
    sector VARCHAR(100),
    is_listed BOOLEAN, 
    updated_at DATETIME, 
    op VARCHAR(10),
    op_ts BIGINT, 
    _ingest_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
PRIMARY KEY(symbol)
DISTRIBUTED BY HASH(symbol) BUCKETS 4 PROPERTIES("replication_num" = "1");

CREATE TABLE IF NOT EXISTS raw_corporate_action(
    symbol VARCHAR(20) NOT NULL,
    ex_date DATE NOT NULL,
    action_type VARCHAR(20) NOT NULL,
    cash_amount DECIMAL(18, 2),
    ratio DECIMAL(10, 6),
    subscription_price DECIMAL(18, 2),
    announce_date DATE,
    updated_at DATETIME,
    op VARCHAR(10),
    op_ts BIGINT,
    _ingest_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
PRIMARY KEY(symbol, ex_date, action_type)
DISTRIBUTED BY HASH(symbol) BUCKETS 4 PROPERTIES("replication_num" = "1");