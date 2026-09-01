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
    'topic' = '{{TRANSACTIONS_RAW_TOPIC}}',
    'properties.bootstrap.servers' = '{{KAFKA_BOOTSTRAP_SERVERS}}',
    'properties.group.id' = '{{KAFKA_GROUP_ID}}',
    'properties.security.protocol' = '{{KAFKA_SECURITY_PROTOCOL}}',
    'properties.sasl.mechanism' = '{{KAFKA_SASL_MECHANISM}}',
    'properties.sasl.jaas.config' = '{{KAFKA_SASL_JAAS_CONFIG}}',
    'properties.sasl.client.callback.handler.class' = '{{KAFKA_SASL_CALLBACK_HANDLER}}',
    'scan.startup.mode' = '{{KAFKA_STARTUP_MODE}}',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);
