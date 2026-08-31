package com.streaming.fraud;

public record ApplicationConfig(
        String bootstrapServers,
        String transactionsRawTopic,
        String cleanTransactionsTopic,
        String fraudAlertsTopic,
        String transactionsDlqTopic,
        String groupId,
        String startupMode,
        String securityProtocol
) {

    public ApplicationConfig {
        bootstrapServers = requireNonBlank(
                bootstrapServers,
                "bootstrapServers"
        );
        transactionsRawTopic = requireNonBlank(
                transactionsRawTopic,
                "transactionsRawTopic"
        );
        cleanTransactionsTopic = requireNonBlank(
                cleanTransactionsTopic,
                "cleanTransactionsTopic"
        );
        fraudAlertsTopic = requireNonBlank(
                fraudAlertsTopic,
                "fraudAlertsTopic"
        );
        transactionsDlqTopic = requireNonBlank(
                transactionsDlqTopic,
                "transactionsDlqTopic"
        );
        groupId = requireNonBlank(groupId, "groupId");
        startupMode = requireNonBlank(startupMode, "startupMode");
        securityProtocol = requireNonBlank(
                securityProtocol,
                "securityProtocol"
        );
    }

    public static ApplicationConfig fromEnvironment() {
        return new ApplicationConfig(
                environmentOrDefault(
                        "KAFKA_BOOTSTRAP_SERVERS",
                        "kafka:29092"
                ),
                environmentOrDefault(
                        "TRANSACTIONS_RAW_TOPIC",
                        "transactions_raw"
                ),
                environmentOrDefault(
                        "CLEAN_TRANSACTIONS_TOPIC",
                        "clean_transactions"
                ),
                environmentOrDefault(
                        "FRAUD_ALERTS_TOPIC",
                        "fraud_alerts"
                ),
                environmentOrDefault(
                        "TRANSACTIONS_DLQ_TOPIC",
                        "transactions_dlq"
                ),
                environmentOrDefault(
                        "KAFKA_GROUP_ID",
                        "transaction-router-v1"
                ),
                environmentOrDefault(
                        "KAFKA_STARTUP_MODE",
                        "latest-offset"
                ),
                environmentOrDefault(
                        "KAFKA_SECURITY_PROTOCOL",
                        "PLAINTEXT"
                )
        );
    }

    private static String environmentOrDefault(
            String variableName,
            String defaultValue
    ) {
        String value = System.getenv(variableName);

        if (value == null || value.isBlank()) {
            return defaultValue;
        }

        return value.trim();
    }

    private static String requireNonBlank(
            String value,
            String fieldName
    ) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    fieldName + " must not be blank"
            );
        }

        return value.trim();
    }
}