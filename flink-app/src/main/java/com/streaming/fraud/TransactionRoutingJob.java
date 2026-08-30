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

        System.out.println("Transaction routing job initialized successfully.");
        System.out.println(
                "Flink parallelism: "
                        + executionEnvironment.getParallelism()
        );

        executeSqlResource(tableEnvironment, "sql/01-source.sql");
        executeSqlResource(tableEnvironment, "sql/02-clean-sink.sql");
        executeSqlResource(tableEnvironment, "sql/03-fraud-sink.sql");
        executeSqlResource(tableEnvironment, "sql/04-dlq-sink.sql");

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
            String resourcePath
    ) throws IOException {
        String sql = readSqlResource(resourcePath);

        System.out.println("Executing SQL resource: " + resourcePath);
        return tableEnvironment.executeSql(sql);
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
