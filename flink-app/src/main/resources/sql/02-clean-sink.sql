CREATE TABLE clean_transactions_sink (
    `transaction_id` STRING,
    `user_id` STRING,
    `amount` DECIMAL(18, 2),
    `country` STRING,
    `event_time` TIMESTAMP_LTZ(3),
    `velocity_score` INT,
    `amount_anomaly_score` INT,
    `spending_burst_score` INT,
    `country_switch_score` INT,
    `risk_score` INT,
    `risk_level` STRING,
    `risk_reasons` ARRAY<STRING>
) WITH (
    'connector' = 'kafka',
    'topic' = 'clean_transactions',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);
