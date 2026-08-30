CREATE TABLE fraud_alerts_sink (
    `transaction_id` STRING,
    `user_id` STRING,
    `amount` DECIMAL(18, 2),
    `country` STRING,
    `event_time` STRING,
    `fraud_reason` STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'fraud_alerts',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format' = 'json'
);
