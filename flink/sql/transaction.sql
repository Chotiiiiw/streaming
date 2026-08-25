create table transactions_raw_source(
    `transaction_id` string,
    `user_id` string, 
    `amount` decimal(18,2), 
    `country` string, 
    `event_time` string,
    `kafka_timestamp` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) with (
    'connector' = 'kafka', 
    'topic' = 'transactions_raw', 
    'properties.bootstrap.servers' = 'kafka:29092', 
    'properties.group.id' = 'transaction-router-v1', 
    'scan.startup.mode' = 'latest-offset', 
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);