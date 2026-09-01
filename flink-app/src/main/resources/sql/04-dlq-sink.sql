CREATE TABLE transactions_dlq_sink (
    `transaction_id` STRING,
    `user_id` STRING,
    `amount` DECIMAL(18, 2),
    `country` STRING,
    `event_time` TIMESTAMP_LTZ(3),
    `error_reason` STRING
) WITH (
    'connector' = 'kafka',
    'topic' = '{{TRANSACTIONS_DLQ_TOPIC}}',
    'properties.bootstrap.servers' = '{{KAFKA_BOOTSTRAP_SERVERS}}',
    'properties.security.protocol' = '{{KAFKA_SECURITY_PROTOCOL}}',
    'properties.sasl.mechanism' = '{{KAFKA_SASL_MECHANISM}}',
    'properties.sasl.jaas.config' = '{{KAFKA_SASL_JAAS_CONFIG}}',
    'properties.sasl.client.callback.handler.class' = '{{KAFKA_SASL_CALLBACK_HANDLER}}',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);
