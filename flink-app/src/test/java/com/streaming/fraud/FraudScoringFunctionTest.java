package com.streaming.fraud;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FraudScoringFunctionTest {

    private static final BigDecimal BASELINE_AVERAGE =
            new BigDecimal("100.00");

    @Test
    void velocityScoreFollowsDoublingBands() {
        assertEquals(0, FraudScoringFunction.velocityScore(2));
        assertEquals(15, FraudScoringFunction.velocityScore(3));
        assertEquals(15, FraudScoringFunction.velocityScore(5));
        assertEquals(30, FraudScoringFunction.velocityScore(6));
        assertEquals(30, FraudScoringFunction.velocityScore(11));
        assertEquals(45, FraudScoringFunction.velocityScore(12));
        assertEquals(60, FraudScoringFunction.velocityScore(24));
    }

    @Test
    void amountScoreRequiresFivePreviousTransactions() {
        assertEquals(
                0,
                FraudScoringFunction.amountAnomalyScore(
                        new BigDecimal("1000.00"),
                        BASELINE_AVERAGE,
                        4
                )
        );
    }

    @Test
    void amountScoreFollowsSeverityBands() {
        assertAmountScore("199.99", 0);
        assertAmountScore("200.00", 10);
        assertAmountScore("300.00", 20);
        assertAmountScore("500.00", 30);
        assertAmountScore("1000.00", 40);
        assertAmountScore("2000.00", 50);
        assertAmountScore("4000.00", 60);
    }

    @Test
    void spendingScoreRequiresFivePreviousTransactions() {
        assertEquals(
                0,
                FraudScoringFunction.spendingBurstScore(
                        new BigDecimal("4000.00"),
                        BASELINE_AVERAGE,
                        4
                )
        );
    }

    @Test
    void spendingScoreFollowsDoublingBands() {
        assertSpendingScore("499.99", 0);
        assertSpendingScore("500.00", 15);
        assertSpendingScore("1000.00", 30);
        assertSpendingScore("2000.00", 45);
        assertSpendingScore("4000.00", 60);
        assertSpendingScore("8000.00", 75);
    }

    @Test
    void countryScoreHasThreeLevels() {
        assertEquals(0, FraudScoringFunction.countrySwitchScore(1));
        assertEquals(30, FraudScoringFunction.countrySwitchScore(2));
        assertEquals(60, FraudScoringFunction.countrySwitchScore(3));
        assertEquals(60, FraudScoringFunction.countrySwitchScore(10));
    }

    private static void assertAmountScore(String amount, int expectedScore) {
        assertEquals(
                expectedScore,
                FraudScoringFunction.amountAnomalyScore(
                        new BigDecimal(amount),
                        BASELINE_AVERAGE,
                        5
                )
        );
    }

    private static void assertSpendingScore(
            String spendingTotal,
            int expectedScore
    ) {
        assertEquals(
                expectedScore,
                FraudScoringFunction.spendingBurstScore(
                        new BigDecimal(spendingTotal),
                        BASELINE_AVERAGE,
                        5
                )
        );
    }
}
