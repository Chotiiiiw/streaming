CREATE TABLE clean_transactions_sink (
    `transaction_id` STRING,
    `user_id` STRING,
    `amount` DECIMAL(18, 2),
    `country` STRING,
    `event_time` STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'clean_transactions',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format' = 'json'
);
