CREATE TABLE transactions_raw_source (
    `transaction_id` STRING,
    `user_id` STRING,
    `amount` DECIMAL(18, 2),
    `country` STRING,
    `event_time` STRING,
    `kafka_timestamp` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
    'connector' = 'kafka',
    'topic' = 'transactions_raw',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'transaction-router-v1',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);
