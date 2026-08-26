create table transactions_raw_source(
    `transaction_id` string,
    `user_id` string, 
    `amount` decimal(18,2), 
    `country` string, 
    `event_time` string,
    `kafka_timestamp` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) with (
    'connector' = 'kafka', 
    'topic' = 'transactions_raw', 
    'properties.bootstrap.servers' = 'kafka:29092', 
    'properties.group.id' = 'transaction-router-v1', 
    'scan.startup.mode' = 'latest-offset', 
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

create table clean_transactions_sink (
    `transaction_id` string,
    `user_id` string,
    `amount` decimal(18,2),
    `country` string,
    `event_time` string
) with (
    'connector' = 'kafka',
    'topic' = 'clean_transactions',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format' = 'json'
);

create table fraud_alerts_sink(
    `transaction_id` string,
    `user_id` string,
    `amount` decimal(18,2),
    `country` string,
    `event_time` string,
    `fraud_reason` string 
) with (
    'connector' = 'kafka',
    'topic' = 'fraud_alerts',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format' = 'json'
);

create table transactions_dlq_sink(
    `transaction_id` string,
    `user_id` string,
    `amount` decimal(18,2),
    `country` string,
    `event_time` string,
    `error_reason` string
) with (
    'connector' = 'kafka',
    'topic' = 'transactions_dlq', 
    'properties.bootstrap.servers' = 'kafka:29092', 
    'format' = 'json'
); 

execute statement set
begin

    insert into clean_transactions_sink
    select
        transaction_id,
        user_id,
        amount,
        country,
        event_time
    from transactions_raw_source
    where
        transaction_id is not null
        and trim(transaction_id) <> ''
        and user_id is not null
        and trim(user_id) <> ''
        and amount > 0
        and country is not null
        and trim(country) <> ''
        and event_time is not null
        and trim(event_time) <> ''
        and amount < 500;

    insert into fraud_alerts_sink
    select
        transaction_id,
        user_id,
        amount,
        country,
        event_time,
        'amount is greater than or equal to 500' as fraud_reason
    from transactions_raw_source
    where
        transaction_id is not null
        and trim(transaction_id) <> ''
        and user_id is not null
        and trim(user_id) <> ''
        and amount > 0
        and country is not null
        and trim(country) <> ''
        and event_time is not null
        and trim(event_time) <> ''
        and amount >= 500;

    insert into transactions_dlq_sink
    select
        transaction_id,
        user_id,
        amount,
        country,
        event_time,
        case
            when transaction_id is null
                 or trim(transaction_id) = ''
                then 'missing transaction_id'
            when user_id is null
                 or trim(user_id) = ''
                then 'missing user_id'
            when amount is null
                then 'amount is missing or invalid'
            when amount <= 0
                then 'amount must be greater than zero'
            when country is null
                 or trim(country) = ''
                then 'missing country'
            when event_time is null
                 or trim(event_time) = ''
                then 'missing event_time'
        end as error_reason
    from transactions_raw_source
    where
        transaction_id is null
        or trim(transaction_id) = ''
        or user_id is null
        or trim(user_id) = ''
        or amount is null
        or amount <= 0
        or country is null
        or trim(country) = ''
        or event_time is null
        or trim(event_time) = '';

end;