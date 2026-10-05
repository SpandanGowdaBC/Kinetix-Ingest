# Kinetix-Ingest ⚡
> **Event Ingestion & Risk Analytics Engine**

[![Java 21](https://img.shields.io/badge/Java-21-orange.svg)](https://www.oracle.com/java/)
[![Spring Boot 3.2](https://img.shields.io/badge/Spring%20Boot-3.2.3-green.svg)](https://spring.io/projects/spring-boot)
[![Apache Kafka](https://img.shields.io/badge/Kafka-Confluent-black.svg)](https://kafka.apache.org/)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-23.8-yellow.svg)](https://clickhouse.com/)

---

## 📌 Executive Summary

**Kinetix-Ingest** is an event ingestion service built with **Java 21** and **Spring Boot 3.2**. It processes telemetry streams from browser extensions, desktop agents, and API security gateways.

It features:
1. **Kafka Consumer (`@KafkaListener`)**: Subscribes to telemetry event topics with thread-safe queue buffering (`ConcurrentLinkedQueue`).
2. **Per-Tenant Strategy Pattern (`EventProcessorFactory`)**: Dynamically resolves risk analysis strategies keyed by tenant ID (`StrictTenantRiskStrategy`, `StandardTenantRiskStrategy`, `DefaultRiskStrategy`).
3. **ClickHouse Batching**: Replaces heavy ORM overhead with `NamedParameterJdbcTemplate` batch inserts, buffering up to configurable batch limits or fixed time-interval flushes.
4. **In-Memory Micro-Benchmark**: In-memory risk processing and queue batching evaluated at **~74,400 events/sec** using a mocked DB writer.

---

## 📊 Measured Load Test Benchmark Results

```text
=========================================================
🔥 IN-MEMORY MICRO-BENCHMARK RESULT (MOCKED CLICKHOUSE WRITER) 🔥
---------------------------------------------------------
Total Events Processed     : 100,000
Total Execution Time       : 1,344 ms (1.344 seconds)
In-Memory Rate (Mocked DB) : ~74,400 events/sec
Average In-Memory Batch Overhead: 13.44 ms/batch (1,000 records/batch)
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

### 3. Run the Test Suite (16 unit tests + 1 benchmark)
```bash
./mvnw test
```

---

## 👨‍💻 Author

**Spandan Gowda B C**
* **GitHub**: [@SpandanGowdaBC](https://github.com/SpandanGowdaBC)
* **Repository**: [Kinetix-Ingest](https://github.com/SpandanGowdaBC/Kinetix-Ingest)
