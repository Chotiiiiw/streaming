package com.streaming.fraud;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public final class TransactionRoutingJob {

    private TransactionRoutingJob() {
        // Prevent accidental construction of this application class.
    }

    public static void main(String[] args) {
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
    }
}