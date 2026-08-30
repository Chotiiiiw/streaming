package com.streaming.fraud.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;

public class Transaction implements Serializable {
    public String transactionId;
    public String userId;
    public BigDecimal amount;
    public String country;
    public Instant eventTime;

    public Transaction() {
    }

    public Transaction(
            String transactionId,
            String userId,
            BigDecimal amount,
            String country,
            Instant eventTime
    ) {
        this.transactionId = transactionId;
        this.userId = userId;
        this.amount = amount;
        this.country = country;
        this.eventTime = eventTime;
    }
}
