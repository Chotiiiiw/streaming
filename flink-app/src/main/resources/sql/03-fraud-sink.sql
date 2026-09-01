CREATE TABLE fraud_alerts_sink (
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
    'topic' = '{{FRAUD_ALERTS_TOPIC}}',
    'properties.bootstrap.servers' = '{{KAFKA_BOOTSTRAP_SERVERS}}',
    'properties.security.protocol' = '{{KAFKA_SECURITY_PROTOCOL}}',
    'properties.sasl.mechanism' = '{{KAFKA_SASL_MECHANISM}}',
    'properties.sasl.jaas.config' = '{{KAFKA_SASL_JAAS_CONFIG}}',
    'properties.sasl.client.callback.handler.class' = '{{KAFKA_SASL_CALLBACK_HANDLER}}',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);
