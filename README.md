# Kinetix-Ingest ⚡
> **High-Throughput Distributed Telemetry Ingestion & Real-Time Risk Analytics Engine**

[![Java 21](https://img.shields.io/badge/Java-21-orange.svg)](https://www.oracle.com/java/)
[![Spring Boot 3.2](https://img.shields.io/badge/Spring%20Boot-3.2.3-green.svg)](https://spring.io/projects/spring-boot)
[![Apache Kafka](https://img.shields.io/badge/Kafka-Confluent-black.svg)](https://kafka.apache.org/)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-23.8-yellow.svg)](https://clickhouse.com/)

---

## 📌 Executive Summary

**Kinetix-Ingest** is a high-speed event ingestion engine built with **Java 21** and **Spring Boot 3.2**. It processes high-throughput telemetry streams from browser extensions, desktop agents, and API security gateways.

It features:
1. **Real-time Kafka Consumer (`@KafkaListener`)**: Subscribes to telemetry event topics with thread-safe queue buffering (`ConcurrentLinkedQueue`).
2. **Per-Tenant Strategy Pattern (`EventProcessorFactory`)**: Dynamically resolves risk analysis strategies keyed by tenant ID (`StrictTenantRiskStrategy`, `StandardTenantRiskStrategy`, `DefaultRiskStrategy`).
3. **High-Speed ClickHouse Batching**: Replaces heavy ORM overhead with `NamedParameterJdbcTemplate` batch inserts, buffering up to configurable batch limits or fixed time-interval flushes.
4. **100,000-Event Measured Load Test Benchmark**: Benchmarked locally at **~74,400 events/sec** with sub-14ms batch write latencies.

---

## 📊 Measured Load Test Benchmark Results

```text
=========================================================
🔥 KINETIX-INGEST 100,000 EVENT LOAD TEST BENCHMARK RESULT 🔥
---------------------------------------------------------
Total Events Processed     : 100,000
Total Execution Time       : 1,344 ms (1.344 seconds)
Measured Ingestion Rate    : ~74,400 events/sec
Average Batch Write Latency: 13.44 ms/batch (1,000 records/batch)
=========================================================
```

---

## 🏗️ System Architecture

```text
┌──────────────────┐
│ Telemetry Events │ (Browser Extensions, Agents, Security Proxies)
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Apache Kafka    │ Topic: kinetix.risk.events
└────────┬─────────┘
         │
         ▼
┌────────────────────────────────────────────────────────┐
│ KafkaTelemetryConsumer (@KafkaListener)                │
│ ├── 1. Per-Tenant Strategy Resolution (Factory)       │
│ │    ├── StrictTenantRiskStrategy (Strict Rules)       │
│ │    ├── StandardTenantRiskStrategy (Standard Rules)   │
│ │    └── DefaultRiskStrategy (Fallback)               │
│ └── 2. Thread-Safe Event Batch Buffer (Size/Time Limit)│
└────────┬───────────────────────────────────────────────┘
         │
         ▼ Batch Write (NamedParameterJdbcTemplate)
┌──────────────────┐
│ ClickHouse OLAP  │ kinetix_db.risk_events
└──────────────────┘
```

---

## 🗄️ ClickHouse Database Setup & DDL

Run the following DDL statements in ClickHouse to initialize the target database and table:

```sql
-- 1. Create Database
CREATE DATABASE IF NOT EXISTS kinetix_db;

-- 2. Create Risk Events MergeTree Table
CREATE TABLE IF NOT EXISTS kinetix_db.risk_events (
    event_id String,
    tenant_id String,
    event_type String,
    source_ip String,
    risk_score Float64,
    created_timestamp UInt64
) ENGINE = MergeTree()
ORDER BY (tenant_id, created_timestamp);
```

---

## 🚀 Quickstart & Setup

### Prerequisites
* Java 21 & Maven 3.8+
* Docker & Docker Compose

### 1. Spin Up Infrastructure (Kafka, Zookeeper, ClickHouse)
```bash
docker-compose up -d
```

### 2. Run the Application
```bash
./mvnw spring-boot:run
```

### 3. Run the Unit & Benchmark Test Suite (16 Tests)
```bash
./mvnw test
```

---

## 👨‍💻 Author

**Spandan Gowda B C**
* **GitHub**: [@SpandanGowdaBC](https://github.com/SpandanGowdaBC)
* **Repository**: [Kinetix-Ingest](https://github.com/SpandanGowdaBC/Kinetix-Ingest)
