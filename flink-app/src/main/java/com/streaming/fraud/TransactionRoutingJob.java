package com.streaming.fraud;

import com.streaming.fraud.model.InvalidTransaction;
import com.streaming.fraud.model.ScoredTransaction;
import com.streaming.fraud.model.Transaction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.table.api.Expressions.$;

public final class TransactionRoutingJob {

    private TransactionRoutingJob() {
        // Prevent accidental construction of this application class.
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance()
                        .inStreamingMode()
                        .build();

        StreamTableEnvironment tableEnvironment =
                StreamTableEnvironment.create(
                        executionEnvironment,
                        environmentSettings
                );
        tableEnvironment.getConfig().setLocalTimeZone(ZoneId.of("UTC"));

        ApplicationConfig applicationConfig =
                ApplicationConfig.fromEnvironment();

        System.out.println("Transaction routing job initialized successfully.");
        System.out.println(
                "Flink parallelism: "
                        + executionEnvironment.getParallelism()
        );

        executeSqlResource(
                tableEnvironment,
                "sql/01-source.sql",
                applicationConfig
        );
        executeSqlResource(
                tableEnvironment,
                "sql/02-clean-sink.sql",
                applicationConfig
        );
        executeSqlResource(
                tableEnvironment,
                "sql/03-fraud-sink.sql",
                applicationConfig
        );
        executeSqlResource(
                tableEnvironment,
                "sql/04-dlq-sink.sql",
                applicationConfig
        );

        Table rawTransactions = tableEnvironment
                .from("transactions_raw_source")
                .select(
                        $("transaction_id"),
                        $("user_id"),
                        $("amount"),
                        $("country"),
                        $("event_time")
                );

        SingleOutputStreamOperator<Transaction> validTransactions =
                tableEnvironment
                        .toDataStream(rawTransactions)
                        .map(TransactionRoutingJob::toTransaction)
                        .returns(Transaction.class)
                        .process(new TransactionValidationFunction());

        DataStream<InvalidTransaction> invalidTransactions =
                validTransactions.getSideOutput(
                        TransactionValidationFunction.INVALID_TRANSACTIONS
                );

        SingleOutputStreamOperator<ScoredTransaction> scoredTransactions =
                validTransactions
                        .keyBy(transaction -> transaction.userId)
                        .process(new FraudScoringFunction());

        DataStream<ScoredTransaction> cleanTransactions = scoredTransactions
                .filter(transaction -> !"HIGH".equals(transaction.riskLevel));

        DataStream<ScoredTransaction> fraudAlerts = scoredTransactions
                .filter(transaction -> "HIGH".equals(transaction.riskLevel));

        StatementSet routingStatements = tableEnvironment.createStatementSet();
        routingStatements.addInsert(
                "clean_transactions_sink",
                scoredTable(tableEnvironment, cleanTransactions)
        );
        routingStatements.addInsert(
                "fraud_alerts_sink",
                scoredTable(tableEnvironment, fraudAlerts)
        );
        routingStatements.addInsert(
                "transactions_dlq_sink",
                invalidTable(tableEnvironment, invalidTransactions)
        );

        TableResult routingResult = routingStatements.execute();

        System.out.println("Transaction routing job submitted successfully.");
        routingResult.await();
    }

    private static Transaction toTransaction(Row row) {
        return new Transaction(
                row.getFieldAs(0),
                row.getFieldAs(1),
                row.getFieldAs(2),
                row.getFieldAs(3),
                row.getFieldAs(4)
        );
    }

    private static Table scoredTable(
            StreamTableEnvironment tableEnvironment,
            DataStream<ScoredTransaction> transactions
    ) {
        return tableEnvironment
                .fromDataStream(transactions)
                .select(
                        $("transactionId").as("transaction_id"),
                        $("userId").as("user_id"),
                        $("amount").cast(DataTypes.DECIMAL(18, 2)),
                        $("country"),
                        $("eventTime")
                                .cast(DataTypes.TIMESTAMP_LTZ(3))
                                .as("event_time"),
                        $("velocityScore").as("velocity_score"),
                        $("amountAnomalyScore")
                                .as("amount_anomaly_score"),
                        $("spendingBurstScore")
                                .as("spending_burst_score"),
                        $("countrySwitchScore")
                                .as("country_switch_score"),
                        $("riskScore").as("risk_score"),
                        $("riskLevel").as("risk_level"),
                        $("riskReasons").as("risk_reasons")
                );
    }

    private static Table invalidTable(
            StreamTableEnvironment tableEnvironment,
            DataStream<InvalidTransaction> transactions
    ) {
        return tableEnvironment
                .fromDataStream(transactions)
                .select(
                        $("transactionId").as("transaction_id"),
                        $("userId").as("user_id"),
                        $("amount").cast(DataTypes.DECIMAL(18, 2)),
                        $("country"),
                        $("eventTime")
                                .cast(DataTypes.TIMESTAMP_LTZ(3))
                                .as("event_time"),
                        $("errorReason").as("error_reason")
                );
    }

    private static TableResult executeSqlResource(
            StreamTableEnvironment tableEnvironment,
            String resourcePath,
            ApplicationConfig applicationConfig
    ) throws IOException {
        String sqlTemplate = readSqlResource(resourcePath);
        String renderedSql = renderSqlTemplate(
                sqlTemplate,
                applicationConfig,
                resourcePath
        );

        System.out.println("Executing SQL resource: " + resourcePath);
        return tableEnvironment.executeSql(renderedSql);
    }

    private static String renderSqlTemplate(
            String sqlTemplate,
            ApplicationConfig config,
            String resourcePath
    ) {
        Map<String, String> replacements = Map.of(
                "KAFKA_BOOTSTRAP_SERVERS",
                config.bootstrapServers(),
                "TRANSACTIONS_RAW_TOPIC",
                config.transactionsRawTopic(),
                "CLEAN_TRANSACTIONS_TOPIC",
                config.cleanTransactionsTopic(),
                "FRAUD_ALERTS_TOPIC",
                config.fraudAlertsTopic(),
                "TRANSACTIONS_DLQ_TOPIC",
                config.transactionsDlqTopic(),
                "KAFKA_GROUP_ID",
                config.groupId(),
                "KAFKA_STARTUP_MODE",
                config.startupMode(),
                "KAFKA_SECURITY_PROTOCOL",
                config.securityProtocol()
        );

        String renderedSql = sqlTemplate;

        for (Map.Entry<String, String> replacement
                : replacements.entrySet()) {
            String placeholder = "{{" + replacement.getKey() + "}}";
            renderedSql = renderedSql.replace(
                    placeholder,
                    escapeSqlValue(replacement.getValue())
            );
        }

        Matcher unresolvedPlaceholder = Pattern
                .compile("\\{\\{[^{}]+}}")
                .matcher(renderedSql);

        if (unresolvedPlaceholder.find()) {
            throw new IllegalArgumentException(
                    "Unresolved SQL placeholder "
                            + unresolvedPlaceholder.group()
                            + " in "
                            + resourcePath
            );
        }

        return renderedSql;
    }

    private static String escapeSqlValue(String value) {
        return value.replace("'", "''");
    }

    private static String readSqlResource(String resourcePath)
            throws IOException {
        ClassLoader classLoader = TransactionRoutingJob.class.getClassLoader();

        try (InputStream inputStream =
                     classLoader.getResourceAsStream(resourcePath)) {
            if (inputStream == null) {
                throw new IllegalArgumentException(
                        "SQL resource not found: " + resourcePath
                );
            }

            return new String(
                    inputStream.readAllBytes(),
                    StandardCharsets.UTF_8
            );
        }
    }
}
