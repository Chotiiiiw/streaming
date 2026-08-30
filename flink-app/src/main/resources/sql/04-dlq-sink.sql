CREATE TABLE transactions_dlq_sink (
    `transaction_id` STRING,
    `user_id` STRING,
    `amount` DECIMAL(18, 2),
    `country` STRING,
    `event_time` TIMESTAMP_LTZ(3),
    `error_reason` STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'transactions_dlq',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);
