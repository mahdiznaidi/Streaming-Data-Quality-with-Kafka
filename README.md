# Kafka Streaming & Data Quality Lab

## Context
In real-world streaming systems, data is often **dirty, inconsistent, and unreliable**.
This lab simulates a realistic Kafka streaming pipeline where students must **clean and validate streaming JSON data**.


## TO DO
Your consumer must:

Parse JSON safely

Validate schema and data types

Detect invalid records

Route records to the appropriate Kafka topic

## Constraints

You must not modify the producer

You must not modify the input files

Your consumer must not crash on bad data

## Running with Docker Compose
A minimal Kafka stack is provided for local testing. Start it with:

```bash
docker compose up -d
```

This launches Zookeeper on `2181` and Kafka on `9092` using the `kafka-lab` network.

## Local Validation
Run the consumer against the provided sample data file:

```bash
python consumer.py --input flights_summary.json --valid-output valid_records.jsonl --invalid-output invalid_records.jsonl
```

## Tests
Install `pytest` if needed (e.g., `pip install pytest`) and run the automated checks:

```bash
pytest
```
