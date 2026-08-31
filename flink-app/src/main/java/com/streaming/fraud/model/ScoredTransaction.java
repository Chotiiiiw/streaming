package com.streaming.fraud.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;

public class ScoredTransaction implements Serializable {
    public String transactionId;
    public String userId;
    public BigDecimal amount;
    public String country;
    public Instant eventTime;
    public int velocityScore;
    public int amountAnomalyScore;
    public int spendingBurstScore;
    public int countrySwitchScore;
    public int riskScore;
    public String riskLevel;
    public String[] riskReasons;

    public ScoredTransaction() {
    }

    public ScoredTransaction(
            Transaction transaction,
            int velocityScore,
            int amountAnomalyScore,
            int spendingBurstScore,
            int countrySwitchScore,
            int riskScore,
            String riskLevel,
            String[] riskReasons
    ) {
        this.transactionId = transaction.transactionId;
        this.userId = transaction.userId;
        this.amount = transaction.amount;
        this.country = transaction.country;
        this.eventTime = transaction.eventTime;
        this.velocityScore = velocityScore;
        this.amountAnomalyScore = amountAnomalyScore;
        this.spendingBurstScore = spendingBurstScore;
        this.countrySwitchScore = countrySwitchScore;
        this.riskScore = riskScore;
        this.riskLevel = riskLevel;
        this.riskReasons = riskReasons;
    }
}
