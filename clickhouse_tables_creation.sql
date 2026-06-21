CREATE TABLE default.crypto_trades_final (
    symbol String,
    price Float64,
    side String,
    size Float64,
    trade_time DateTime64(3, 'UTC')
) ENGINE = MergeTree()
ORDER BY (symbol, trade_time);


CREATE TABLE default.crypto_trades_queue (
    symbol String,
    price Float64,
    side String,
    size Float64,
    trade_time Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'crypto_trades',
    kafka_group_name = 'clickhouse_consumers',
    kafka_format = 'JSONEachRow';


CREATE MATERIALIZED VIEW default.mv_crypto_trades_consumer TO default.crypto_trades_final AS
SELECT
    symbol,
    price,
    side,
    size,
    FROM_UNIXTIME(toInt64(trade_time / 1000)) AS trade_time
FROM default.crypto_trades_queue;