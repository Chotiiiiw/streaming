package com.streaming.fraud.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;

public class InvalidTransaction implements Serializable {
    public String transactionId;
    public String userId;
    public BigDecimal amount;
    public String country;
    public Instant eventTime;
    public String errorReason;

    public InvalidTransaction() {
    }

    public InvalidTransaction(Transaction transaction, String errorReason) {
        this.transactionId = transaction.transactionId;
        this.userId = transaction.userId;
        this.amount = transaction.amount;
        this.country = transaction.country;
        this.eventTime = transaction.eventTime;
        this.errorReason = errorReason;
    }
}
