EXECUTE STATEMENT SET
BEGIN
    INSERT INTO clean_transactions_sink
    SELECT
        transaction_id,
        user_id,
        amount,
        country,
        event_time
    FROM transactions_raw_source
    WHERE
        transaction_id IS NOT NULL
        AND TRIM(transaction_id) <> ''
        AND user_id IS NOT NULL
        AND TRIM(user_id) <> ''
        AND amount > 0
        AND country IS NOT NULL
        AND TRIM(country) <> ''
        AND event_time IS NOT NULL
        AND TRIM(event_time) <> ''
        AND amount < 500;

    INSERT INTO fraud_alerts_sink
    SELECT
        transaction_id,
        user_id,
        amount,
        country,
        event_time,
        'amount is greater than or equal to 500' AS fraud_reason
    FROM transactions_raw_source
    WHERE
        transaction_id IS NOT NULL
        AND TRIM(transaction_id) <> ''
        AND user_id IS NOT NULL
        AND TRIM(user_id) <> ''
        AND amount > 0
        AND country IS NOT NULL
        AND TRIM(country) <> ''
        AND event_time IS NOT NULL
        AND TRIM(event_time) <> ''
        AND amount >= 500;

    INSERT INTO transactions_dlq_sink
    SELECT
        transaction_id,
        user_id,
        amount,
        country,
        event_time,
        CASE
            WHEN transaction_id IS NULL OR TRIM(transaction_id) = ''
                THEN 'missing transaction_id'
            WHEN user_id IS NULL OR TRIM(user_id) = ''
                THEN 'missing user_id'
            WHEN amount IS NULL
                THEN 'amount is missing or invalid'
            WHEN amount <= 0
                THEN 'amount must be greater than zero'
            WHEN country IS NULL OR TRIM(country) = ''
                THEN 'missing country'
            WHEN event_time IS NULL OR TRIM(event_time) = ''
                THEN 'missing event_time'
        END AS error_reason
    FROM transactions_raw_source
    WHERE
        transaction_id IS NULL
        OR TRIM(transaction_id) = ''
        OR user_id IS NULL
        OR TRIM(user_id) = ''
        OR amount IS NULL
        OR amount <= 0
        OR country IS NULL
        OR TRIM(country) = ''
        OR event_time IS NULL
        OR TRIM(event_time) = '';
END;
