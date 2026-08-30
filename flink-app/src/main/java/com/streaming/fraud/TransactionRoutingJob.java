package com.streaming.fraud;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

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

        System.out.println("Transaction routing job initialized successfully.");
        System.out.println(
                "Flink parallelism: "
                        + executionEnvironment.getParallelism()
        );

        executeSqlResource(tableEnvironment, "sql/01-source.sql");
        executeSqlResource(tableEnvironment, "sql/02-clean-sink.sql");
        executeSqlResource(tableEnvironment, "sql/03-fraud-sink.sql");
        executeSqlResource(tableEnvironment, "sql/04-dlq-sink.sql");

        TableResult routingResult =
                executeSqlResource(tableEnvironment, "sql/05-routing.sql");

        System.out.println("Transaction routing job submitted successfully.");
        routingResult.await();
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
