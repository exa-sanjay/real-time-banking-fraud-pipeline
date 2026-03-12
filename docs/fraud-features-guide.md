# Fraud Features Guide

## Purpose

`ANALYTICS.FRAUD_FEATURES` is the ML-ready transaction feature table used by the Exasol fraud demo.

Think of it as:

- one row per transaction
- raw transaction context plus derived fraud indicators
- optional ground-truth label from investigations
- model output written back into the same table

The table is defined in `01_exasol_schema.sql` and populated in `02_features_and_udfs.sql`.

## How To Read The Table

The easiest way to read a row is:

1. identify the transaction
2. look at the behavior features
3. look at the risk flags
4. look at the fraud score

In practice:

- `TXN_ID` tells you which transaction is being scored
- `AMOUNT_USD`, `CHANNEL`, `MERCHANT_MCC` describe the transaction itself
- velocity and deviation columns explain why it may be unusual
- risk flags show suspicious patterns like cross-border or new device usage
- `FRAUD_SCORE` is the model output

## Recommended Demo Query

Use this query during demos:

```sql
SELECT
    TXN_ID,
    AMOUNT_USD,
    CHANNEL,
    MERCHANT_MCC,
    TXN_COUNT_1H,
    TXN_COUNT_24H,
    AMOUNT_VS_AVG_RATIO,
    IS_CROSS_BORDER,
    IS_NEW_DEVICE_30D,
    FRAUD_SCORE,
    MODEL_VERSION
FROM ANALYTICS.FRAUD_FEATURES
WHERE FRAUD_SCORE IS NOT NULL
ORDER BY FRAUD_SCORE DESC, FEATURE_COMPUTED_AT DESC
LIMIT 10;
```

This keeps only the fields that help explain the score quickly.

## Column Groups

### Identity

- `TXN_ID`: transaction identifier
- `ACCOUNT_ID`: account linked to the transaction
- `CUSTOMER_ID`: customer linked to the transaction

### Transaction Context

- `AMOUNT_USD`: transaction amount
- `TXN_TYPE`: purchase, transfer, withdrawal, etc.
- `CHANNEL`: channel such as `ONLINE`, `MOBILE`, `ATM`, `API`
- `MERCHANT_MCC`: merchant category code
- `KYC_STATUS`: customer KYC state
- `RISK_BAND`: customer risk band

These columns describe what happened and who it happened to.

### Velocity Features

- `TXN_COUNT_1H`: prior transaction count in the last hour
- `TXN_COUNT_6H`: prior transaction count in the last 6 hours
- `TXN_COUNT_24H`: prior transaction count in the last 24 hours
- `AMOUNT_SUM_1H`: amount spent in the prior hour
- `AMOUNT_SUM_6H`: amount spent in the prior 6 hours
- `AMOUNT_SUM_24H`: amount spent in the prior 24 hours

These columns answer:

- “Is this transaction part of an unusual burst of activity?”

### Deviation Features

- `ACCOUNT_AVG_AMOUNT_30D`: account's historical average amount
- `AMOUNT_VS_AVG_RATIO`: current amount divided by historical average

These columns answer:

- “Is this transaction much larger than normal for this account?”

### Risk Flags

- `IS_CROSS_BORDER`: transaction country differs from expected customer country context
- `IS_NEW_COUNTRY_30D`: this country has not been seen recently for this account
- `IS_NEW_DEVICE_30D`: this device has not been seen recently
- `IS_NIGHT_TXN`: happened during the night window
- `IS_WEEKEND_TXN`: happened on a weekend
- `MCC_BASE_RISK`: built-in risk weight for the merchant category

These are the easiest columns to explain to an audience because they map directly to common fraud signals.

### ML / Outcome Columns

- `FRAUD_SCORE`: model output from `0` to `1`
- `MODEL_VERSION`: which model generated the score
- `FRAUD_LABEL`: known investigated outcome if available
- `FEATURE_COMPUTED_AT`: when the feature row was last refreshed

## How To Explain A High-Scoring Row

Example explanation:

- `TXN_COUNT_1H` is high
- `AMOUNT_VS_AVG_RATIO` is high
- `IS_CROSS_BORDER = TRUE`
- `IS_NEW_DEVICE_30D = TRUE`
- `MCC_BASE_RISK` is high

That means:

- the customer is suddenly very active
- the amount is unusual for their normal behavior
- the location and device are not familiar
- the merchant category is inherently risky

So a higher `FRAUD_SCORE` is easy to justify.

## Good Audience Narrative

Use this explanation:

- “This table is where raw events become fraud intelligence.”
- “Each row is one transaction.”
- “The columns describe both the transaction itself and the behavioral signals around it.”
- “The final fraud score is computed inside Exasol and written back into the same analytical table.”

## Helpful Queries

### Top suspicious transactions

```sql
SELECT
    TXN_ID,
    ACCOUNT_ID,
    AMOUNT_USD,
    CHANNEL,
    MERCHANT_MCC,
    FRAUD_SCORE,
    MODEL_VERSION
FROM ANALYTICS.FRAUD_FEATURES
WHERE FRAUD_SCORE IS NOT NULL
ORDER BY FRAUD_SCORE DESC
LIMIT 10;
```

### Compare model output with known labels

```sql
SELECT
    TXN_ID,
    FRAUD_SCORE,
    FRAUD_LABEL,
    MODEL_VERSION
FROM ANALYTICS.FRAUD_FEATURES
WHERE FRAUD_LABEL IS NOT NULL
ORDER BY FRAUD_SCORE DESC;
```

### Show why a row is suspicious

```sql
SELECT
    TXN_ID,
    AMOUNT_USD,
    TXN_COUNT_1H,
    TXN_COUNT_24H,
    AMOUNT_VS_AVG_RATIO,
    IS_CROSS_BORDER,
    IS_NEW_COUNTRY_30D,
    IS_NEW_DEVICE_30D,
    IS_NIGHT_TXN,
    MCC_BASE_RISK,
    FRAUD_SCORE
FROM ANALYTICS.FRAUD_FEATURES
ORDER BY FRAUD_SCORE DESC
LIMIT 10;
```
