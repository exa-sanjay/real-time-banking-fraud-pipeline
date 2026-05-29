# Customer Demo Brief

## What This Project Demonstrates

This project shows how a bank or fintech can move operational transaction data into an analytical platform in near real time and score fraud with a governed, explainable pipeline.

In simple terms:

- a transaction happens in PostgreSQL
- the change is captured automatically
- the event is streamed through Kafka
- Exasol receives the event for analytics
- fraud features are calculated
- an uploaded fraud model is executed inside Exasol

## Business Story

The project is designed to answer a common customer question:

“How do we get from live banking transactions to trusted analytics and fraud scoring without building a fragile batch pipeline?”

The answer shown by this demo is:

- capture changes once at the source
- stream them reliably through Kafka
- use schema-managed events
- land them in Exasol for analytics
- compute fraud features and scores close to the data

## Main Value Points

### Near real-time movement

The pipeline is event-driven rather than batch-only.

Why this matters:

- fresher risk visibility
- faster alerting
- fewer delays between operations and analytics

### Decoupled architecture

Kafka sits between the source database and Exasol.

Why this matters:

- the source system is not tightly coupled to the warehouse
- additional consumers can be added later
- the streaming layer can scale independently

### Governed data contracts

In Avro mode, schemas are registered in Schema Registry.

Why this matters:

- producer and consumer agree on structure
- schema changes are easier to manage
- downstream integrations are more reliable

### Exasol-centered analytics

Exasol is not only a storage target here. It becomes the analytical execution layer.

Why this matters:

- warehouse tables are organized into RAW, CLEANSED, and ANALYTICS layers
- feature engineering happens in SQL
- model scoring can happen inside Exasol through UDFs

## Demo Narrative

Use this talk track during a demo:

1. “This PostgreSQL database represents the live transactional banking system.”
2. “Debezium listens to the database change log, so no custom polling is needed.”
3. “Each change is published into Kafka as an event.”
4. “In the showcase mode, the events are serialized with Avro and governed through Schema Registry.”
5. “Exasol consumes those events, lands them, and organizes them into analytical layers.”
6. “We then derive fraud features such as transaction velocity, cross-border activity, new device usage, and night-time patterns.”
7. “Finally, Exasol can score those transactions using one uploaded ML model inside the database.”

## What Each Platform Is Doing

### PostgreSQL

Represents the live banking application database.

Typical customer phrasing:

- “This is where transactions are created and updated.”

### Debezium

Captures database changes as events.

Typical customer phrasing:

- “This is the CDC engine that converts row changes into event streams.”

### Kafka

Acts as the event backbone.

Typical customer phrasing:

- “Kafka is the transport layer that decouples producers from consumers.”

### Schema Registry

Manages Avro schemas for streaming messages.

Typical customer phrasing:

- “This gives us governed event contracts with Avro and Schema Registry.”

### Exasol

Acts as the analytical and scoring platform.

Typical customer phrasing:

- “Exasol is where landed data becomes analytics, features, and model-driven outputs.”

### Kafka UI

Provides observability.

Typical customer phrasing:

- “This is how we show the live stream, schemas, and connector status during the demo.”

## What To Show Live

The strongest live demo sequence is:

1. Show a new transaction inserted into PostgreSQL.
2. Show the Debezium connector running in Kafka Connect.
3. Show the Avro topic and schema in Kafka UI.
4. Show the imported record in Exasol.
5. Show the transformed record in the analytical layer.
6. Show the fraud feature table and resulting fraud score.

## Why This Architecture Is Credible

This is not a toy one-table ingest demo. It includes:

- multiple banking entities
- CDC rather than flat-file loading
- streaming infrastructure
- schema governance
- warehouse layering
- feature engineering
- ML model deployment mechanics

That makes it a strong showcase for customers evaluating:

- real-time analytics
- fraud monitoring
- governed data pipelines
- Exasol as an event-to-analytics target

## Recommended Showcase Configuration

For customer demos, the best story is:

```bash
bash deploy.sh
bash prepare_exasol_docker.sh
```

Why this mode is strongest:

- Avro + Schema Registry shows governance
- Debezium shows true CDC
- the official Exasol connector shows Exasol-native integration
- Exasol layers and UDFs show analytical depth

## Short Executive Summary

If you need a very short explanation:

“This solution captures banking transactions from PostgreSQL in real time, streams them through Kafka with governed Avro schemas, loads them into Exasol for analytics, and computes fraud signals close to the data. It demonstrates a practical path from operational events to analytical and ML-ready outcomes.”
