CREATE TABLE transactions_raw_source (
    `transaction_id` STRING,
    `user_id` STRING,
    `amount` DECIMAL(18, 2),
    `country` STRING,
    `event_time` TIMESTAMP_LTZ(3),
    `kafka_timestamp` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
    WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'transactions_raw',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'transaction-router-v1',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);
