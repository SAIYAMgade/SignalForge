# SignalForge
### Production Data Engineering, Pipeline Reliability & Observability Control Plane

[![Python](https://img.shields.io/badge/Python-3.11%20%7C%203.12%20%7C%203.14-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-009688?style=flat&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?style=flat&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8+-017CEE?style=flat&logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![Prometheus](https://img.shields.io/badge/Prometheus-Metrics-E6522C?style=flat&logo=prometheus&logoColor=white)](https://prometheus.io/)
[![Grafana](https://img.shields.io/badge/Grafana-Dashboards-F46800?style=flat&logo=grafana&logoColor=white)](https://grafana.com/)
[![React](https://img.shields.io/badge/React-19%20%2B%20TypeScript-61DAFB?style=flat&logo=react&logoColor=black)](https://react.dev/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker&logoColor=white)](https://www.docker.com/)
[![Tests](https://img.shields.io/badge/Tests-26%20Passed%20(100%25)-success?style=flat&logo=pytest&logoColor=white)](tests/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**SignalForge** is a production data engineering control plane that allows engineering teams to ingest data across heterogeneous sources, execute resilient ETL pipelines, enforce declarative quality contracts, detect schema drift, investigate failure root causes deterministically, and observe operations in real time.

Built adhering to the **Ponytail** senior developer principle: minimal, maintainable architecture with zero speculative bloat, complete test coverage, and strict transaction safety.

---

## 1. System Architecture

```mermaid
flowchart TD
    subgraph Ingestion [1. Multi-Source Ingestion]
        CSV[CSV Files]
        JSON[JSON / JSON Lines]
        REST[REST APIs with Backoff]
        PG[(PostgreSQL / SQLite Tables)]
        Stream[Simulated Event Streams]
    end

    subgraph CoreEngine [2. SignalForge Pipeline Engine]
        direction TB
        E1[EXTRACT: Incremental Cursor & Checkpoints]
        E2[VALIDATE: Schema Drift & 10 Quality Rules]
        E3[TRANSFORM: Mapping, Type Normalization & Cleaning]
        E4[LOAD: Transactional Idempotent UPSERT]
        E5[VERIFY: Post-Load Reconciliation Audit]
        E6[RECORD: Metrics Exposition & Status Update]
        E1 --> E2 --> E3 --> E4 --> E5 --> E6
    end

    subgraph Reliability [3. Reliability Plane]
        Quality[Data Quality Engine: Record Quarantine vs Batch Fail]
        Drift[Schema Drift Detector: Breaking vs Compatible]
        Diag[Deterministic Diagnostic Engine]
        Lab[Failure Recovery Laboratory: 8 Scenarios]
        Prom[Prometheus Metrics Exporter]
    end

    subgraph Storage [4. Persistence & Metadata]
        MetaDB[(Control Plane Metadata DB)]
        TargetTables[(dim_customers & fact_transactions)]
    end

    Ingestion --> CoreEngine
    CoreEngine <--> Reliability
    CoreEngine --> Storage
    Reliability --> Storage
```

---

## 2. Pipeline Lifecycle & Latency Breakdown

Every pipeline execution follows an explicit 6-stage lifecycle with atomic checkpoint updates and failure rollback:

| Stage | Responsibility | Enforcement / Behavior | Output |
|---|---|---|---|
| **`1. EXTRACT`** | Multi-source batch ingestion | Incremental cursors (`cursor_column`), bounded retries | Raw DataFrame + Checkpoint |
| **`2. VALIDATE`** | Schema drift & quality checks | Evaluates 10 rules; quarantines bad records | Clean DataFrame + `rejected_records` |
| **`3. TRANSFORM`** | Field mapping & type conversion | ISO timestamp normalization, string trimming, defaults | Target Schema DataFrame |
| **`4. LOAD`** | Database persistence | Transactional **UPSERT** (`ON CONFLICT DO UPDATE`) | Target Table Rows + Checkpoint |
| **`5. VERIFY`** | Post-load reconciliation | Asserts loaded row counts match transformed counts | Verified State |
| **`6. RECORD`** | Operational observability | Exposes Prometheus counters, emits structured JSON | Run Record + Latency Metric |

![Pipeline Stage Latency Breakdown](docs/assets/pipeline_latency_breakdown.png)

---

## 3. Declarative Data Quality Engine

SignalForge evaluates 10 vectorized quality checks per batch with configurable severity policies:

| Rule Type | Description | Target Columns | Severity Policy |
|---|---|---|---|
| **`NOT_NULL`** | Rejects null or empty string values | `customer_id`, `transaction_id`, `email` | `BATCH_FAIL` / `RECORD_REJECT` |
| **`UNIQUE`** | Prevents duplicate primary keys in batch | `customer_id`, `transaction_id` | `RECORD_REJECT` / `BATCH_FAIL` |
| **`TYPE_CHECK`** | Validates castability to integer, float, datetime | `amount`, `signup_date` | `RECORD_REJECT` |
| **`NON_NEGATIVE`**| Enforces non-negative financial values | `amount >= 0` | `RECORD_REJECT` |
| **`VALID_TIMESTAMP`**| Verifies ISO 8601 datetime syntax | `transaction_timestamp` | `RECORD_REJECT` |
| **`REFERENTIAL_INTEGRITY`**| Verifies foreign key existence in dimension table | `fact_transactions.customer_id` | `RECORD_REJECT` |
| **`CATEGORY_WHITELIST`**| Restricts values to allowed whitelist set | `status IN ('COMPLETED', 'PENDING', ...)` | `RECORD_REJECT` |
| **`RECORD_COUNT_DEVIATION`**| Flags volume anomalies outside expected bounds | `min_count` to `max_count` | `BATCH_FAIL` |
| **`DATA_FRESHNESS`**| Detects pipeline stalls when max timestamp > SLA | `max_age_hours <= 24h` | `RECORD_REJECT` |
| **`SCHEMA_COMPATIBILITY`**| Checks presence of all mandatory columns | Dataset contract check | `BATCH_FAIL` |

---

## 4. Failure Recovery Laboratory & Deterministic Diagnostics

SignalForge provides a built-in test laboratory that deliberately injects 8 reproducible failure modes. When a run fails, the **Deterministic Diagnostic Engine** analyzes the context and isolates empirical facts, likely causes, and recommended actions without LLM hallucination:

| Scenario | Injected Fault | Detected Stage | Error Category | Automated Recovery Action |
|---|---|---|---|---|
| **`duplicate_records`** | Duplicate primary keys | `VALIDATE` | `DATA_QUALITY_VIOLATION` | Quarantine duplicates to `rejected_records`; load unique rows. |
| **`missing_required_fields`** | Null mandatory columns | `VALIDATE` | `DATA_QUALITY_VIOLATION` | Trigger `BATCH_FAIL` rollback to protect downstream consistency. |
| **`breaking_schema_drift`** | Required column dropped | `VALIDATE` | `SCHEMA_DRIFT` | Halt ingestion before schema corruption; alert data engineering. |
| **`malformed_input`** | Truncated invalid JSON | `EXTRACT` | `SOURCE_FORMAT_ERROR` | Quarantine file to dead-letter queue; log parser exception. |
| **`unavailable_api`** | Upstream HTTP 503 outage | `EXTRACT` | `UPSTREAM_UNAVAILABLE` | Execute bounded exponential backoff (3 attempts); trip breaker. |
| **`referential_integrity`** | Orphan foreign keys | `VALIDATE` | `DATA_QUALITY_VIOLATION` | Enforce DAG dependencies (customer dimension before facts). |
| **`stale_data`** | Feeds older than 72 hours | `VALIDATE` | `DATA_QUALITY_VIOLATION` | Flag freshness SLA violation; trigger producer scheduler audit. |
| **`interrupted_run_retry`** | Identical batch re-run | `VERIFY` | `NONE` (Pass) | **Zero duplicate rows created (100% Idempotent UPSERT).** |

![Failure Recovery Matrix](docs/assets/failure_recovery_matrix.png)

---

## 5. Performance Benchmark

Measured on a 3,000 transaction batch using `python scripts/benchmark.py --size 3000`:

| Benchmark Metric | Measured Result | Production Standard |
|---|---|---|
| **Processing Throughput** | **306.6 records / second** | Single-node commodity worker |
| **Batch Latency (3k records)** | **3.26 seconds** | End-to-end Extract → Load → Verify |
| **Peak Memory Footprint** | **4.27 MB** | Streaming generator & batch processing |
| **Duplicate Records on Re-Run** | **0 duplicates (100% Idempotent)** | Database-level unique constraint + UPSERT |
| **Failure Scenarios Handled** | **8 / 8 passed (100% recovery rate)** | Deterministic diagnostic verification |

*Benchmark reports are persisted to [`reports/benchmark_report.json`](reports/benchmark_report.json).*

---

## 6. Quickstart Guide

### Option A: Local Standalone Development (Zero Docker Required)

```bash
# 1. Install dependencies
pip install -r requirements.txt
npm install --prefix frontend

# 2. Seed database & run initial pipelines
python scripts/seed_demo.py

# 3. Start API & Control Plane UI (serves at http://localhost:8000)
python -m uvicorn signalforge.api.app:app --host 0.0.0.0 --port 8000
```

- **Control Plane UI**: [`http://localhost:8000`](http://localhost:8000)
- **FastAPI OpenAPI Swagger**: [`http://localhost:8000/docs`](http://localhost:8000/docs)
- **Prometheus Metrics**: [`http://localhost:8000/metrics`](http://localhost:8000/metrics)

### Option B: Docker Compose Full Stack

```bash
# Start PostgreSQL 16, SignalForge Backend, Airflow, Prometheus, and Grafana
docker compose up -d --build
```

- **SignalForge UI**: [`http://localhost:8000`](http://localhost:8000)
- **Grafana Dashboard**: [`http://localhost:3000`](http://localhost:3000) *(User: `admin`, Password: `admin`)*
- **Prometheus UI**: [`http://localhost:9090`](http://localhost:9090)
- **Apache Airflow**: [`http://localhost:8080`](http://localhost:8080)

---

## 7. Automated Test Suite

SignalForge includes unit, integration, API, and end-to-end tests:

```bash
# Run complete test suite
python -m pytest -v

# Run with test coverage report
python -m pytest -v --cov=signalforge --cov-report=term-missing tests/
```

```
====================== 26 passed in 2.32s ======================
```

Coverage highlights:
- **Unit Tests**: Field mappings, type normalization, all 10 quality checks, schema drift classification, diagnostic rule matching.
- **Integration Tests**: SafeLoader upserts, transaction rollbacks on failure, cursor checkpoints, all 8 Failure Lab scenarios, REST API routes.
- **End-to-End Test**: Customer master ingestion → Transaction ingestion with faulty records → Quarantine to `rejected_records` → Idempotent re-run → Zero duplicate creation.

---

## 8. Airflow Orchestration DAG

The DAG `signalforge_daily_etl` ([`airflow/dags/signalforge_etl_dag.py`](airflow/dags/signalforge_etl_dag.py)) provides scheduled production execution:
1. `check_environment`: Validates database connectivity and storage.
2. `run_customers_etl`: Executes `customer_dimension_pipeline`, recording Airflow run ID into SignalForge metadata.
3. `run_transactions_etl`: Executes `transaction_facts_pipeline` with retries and exponential backoff.
4. `reconcile_and_verify`: Verifies post-load relational integrity across tables.

---

## 9. Architecture Decisions & Threat Model

- [ADR 001: Declarative In-Engine Data Quality vs Heavy Validation Frameworks](docs/adr/001_deterministic_quality_engine.md)
- [ADR 002: Idempotent UPSERT & Atomic Checkpoint Persistence](docs/adr/002_idempotent_loading_strategy.md)
- [Threat Model & Security Policy](docs/threat_model.md)

---

## 10. License

MIT License. See [LICENSE](LICENSE) for details.
